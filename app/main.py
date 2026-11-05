import json
import logging
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, HTTPException, Request, status
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import Settings
from app.schemas import (
    ALLOWED_TRANSITIONS,
    CreateOrderRequest,
    OrderAcceptedResponse,
    OrderEvent,
    OrderStatus,
    OrderStatusResponse,
    UpdateOrderRequest,
)
from app.store import order_status_key

logger = logging.getLogger(__name__)


def create_app(
    producer: Any | None = None,
    redis_client: Any | None = None,
    settings: Settings | None = None,
) -> FastAPI:
    config = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        owns_producer = producer is None
        owns_redis = redis_client is None
        app.state.producer = producer or AIOKafkaProducer(
            bootstrap_servers=config.kafka_bootstrap_servers
        )
        app.state.redis = redis_client or Redis.from_url(
            config.redis_url, decode_responses=True
        )
        try:
            if owns_producer:
                await app.state.producer.start()
            yield
        finally:
            if owns_producer:
                await app.state.producer.stop()
            if owns_redis:
                await app.state.redis.aclose()

    app = FastAPI(title="Order Tracker API", version="0.1.0", lifespan=lifespan)
    if producer is not None:
        app.state.producer = producer
    if redis_client is not None:
        app.state.redis = redis_client

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/orders",
        response_model=OrderAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def create_order(
        payload: CreateOrderRequest, request: Request
    ) -> OrderAcceptedResponse:
        order_id = uuid4()
        event = OrderEvent(
            order_id=order_id,
            status=OrderStatus.PENDING,
            details=payload.model_dump(mode="json"),
        )
        await publish_event(request, event, config)
        return OrderAcceptedResponse(order_id=order_id, status=event.status)

    @app.patch(
        "/orders/{order_id}/status",
        response_model=OrderAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def update_order_status(
        order_id: UUID, payload: UpdateOrderRequest, request: Request
    ) -> OrderAcceptedResponse:
        try:
            raw_status = await request.app.state.redis.get(order_status_key(order_id))
        except RedisError as exc:
            logger.exception("Could not read the status for order %s", order_id)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Order status store is unavailable",
            ) from exc
        if raw_status is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found in the status store yet",
            )

        try:
            current_status = OrderStatus(json.loads(raw_status)["status"])
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Stored order status is invalid",
            ) from None

        if payload.status not in ALLOWED_TRANSITIONS[current_status]:
            allowed = sorted(item.value for item in ALLOWED_TRANSITIONS[current_status])
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Cannot transition order from {current_status.value} "
                    f"to {payload.status.value}. Allowed: {allowed}"
                ),
            )

        event = OrderEvent(order_id=order_id, status=payload.status)
        await publish_event(request, event, config)
        return OrderAcceptedResponse(order_id=order_id, status=event.status)

    @app.get("/orders/{order_id}", response_model=OrderStatusResponse)
    async def get_order_status(order_id: UUID, request: Request) -> OrderStatusResponse:
        try:
            raw_status = await request.app.state.redis.get(order_status_key(order_id))
        except RedisError as exc:
            logger.exception("Could not read the status for order %s", order_id)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Order status store is unavailable",
            ) from exc
        if raw_status is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order status is not available yet",
            )
        try:
            return OrderStatusResponse.model_validate_json(raw_status)
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Stored order status is invalid",
            ) from None

    return app


async def publish_event(request: Request, event: OrderEvent, settings: Settings) -> None:
    try:
        await request.app.state.producer.send_and_wait(
            settings.kafka_topic,
            key=str(event.order_id).encode("utf-8"),
            value=event.model_dump_json().encode("utf-8"),
        )
    except KafkaError as exc:
        logger.exception("Could not publish event %s", event.event_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event broker is unavailable",
        ) from exc


app = create_app()
