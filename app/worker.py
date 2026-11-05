import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from redis.asyncio import Redis

from app.config import Settings
from app.schemas import OrderEvent, OrderStatusResponse
from app.store import order_status_key

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_event(value: bytes, redis_client: Redis) -> None:
    event = OrderEvent.model_validate_json(value)
    status_record = OrderStatusResponse(
        order_id=event.order_id,
        status=event.status,
        updated_at=event.occurred_at,
    )
    await redis_client.set(
        order_status_key(event.order_id), status_record.model_dump_json()
    )


async def run_worker() -> None:
    settings = Settings.from_env()
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    await consumer.start()
    logger.info("Consuming topic %s", settings.kafka_topic)
    try:
        async for message in consumer:
            await process_event(message.value, redis_client)
            await consumer.commit()
    finally:
        await consumer.stop()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(run_worker())
