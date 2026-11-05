import json
from datetime import datetime, timezone
from uuid import UUID

from fastapi.testclient import TestClient

from app.main import create_app, order_status_key
from app.schemas import OrderEvent, OrderStatus


class FakeProducer:
    def __init__(self):
        self.events = []

    async def send_and_wait(self, topic, key, value):
        self.events.append((topic, key, value))


class FakeRedis:
    def __init__(self):
        self.values = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value):
        self.values[key] = value


def make_client():
    producer = FakeProducer()
    redis_client = FakeRedis()
    app = create_app(producer=producer, redis_client=redis_client)
    return TestClient(app), producer, redis_client


def test_create_order_publishes_initial_event():
    client, producer, _ = make_client()
    response = client.post(
        "/orders",
        json={
            "customer_id": "customer-42",
            "items": [{"sku": "book-1", "quantity": 2}],
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert len(producer.events) == 1
    topic, key, value = producer.events[0]
    event = OrderEvent.model_validate_json(value)
    assert topic == "order-events"
    assert key.decode() == body["order_id"]
    assert event.order_id == UUID(body["order_id"])
    assert event.status == OrderStatus.PENDING
    assert event.details["customer_id"] == "customer-42"


def test_create_order_rejects_invalid_items():
    client, producer, _ = make_client()
    response = client.post(
        "/orders", json={"customer_id": "customer-42", "items": []}
    )

    assert response.status_code == 422
    assert producer.events == []


def test_status_read_and_valid_transition_publish_event():
    client, producer, redis_client = make_client()
    order_id = "016d84d5-79f8-41a3-a6c3-d8da55b4b581"
    redis_client.values[order_status_key(UUID(order_id))] = json.dumps(
        {
            "order_id": order_id,
            "status": "pending",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    read_response = client.get(f"/orders/{order_id}")
    assert read_response.status_code == 200
    assert read_response.json()["status"] == "pending"

    update_response = client.patch(
        f"/orders/{order_id}/status", json={"status": "confirmed"}
    )
    assert update_response.status_code == 202
    assert update_response.json()["status"] == "confirmed"
    assert len(producer.events) == 1
    event = OrderEvent.model_validate_json(producer.events[0][2])
    assert event.order_id == UUID(order_id)
    assert event.status == OrderStatus.CONFIRMED


def test_update_rejects_invalid_transition():
    client, producer, redis_client = make_client()
    order_id = "016d84d5-79f8-41a3-a6c3-d8da55b4b581"
    redis_client.values[order_status_key(UUID(order_id))] = json.dumps(
        {
            "order_id": order_id,
            "status": "pending",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    response = client.patch(
        f"/orders/{order_id}/status", json={"status": "shipped"}
    )

    assert response.status_code == 409
    assert producer.events == []


def test_status_read_returns_not_found_before_worker_updates_redis():
    client, _, _ = make_client()

    response = client.get("/orders/016d84d5-79f8-41a3-a6c3-d8da55b4b581")

    assert response.status_code == 404
