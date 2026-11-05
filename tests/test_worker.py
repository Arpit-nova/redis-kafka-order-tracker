import asyncio
import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from app.main import order_status_key
from app.schemas import OrderEvent, OrderStatus
from app.worker import process_event


class FakeRedis:
    def __init__(self):
        self.values = {}

    async def set(self, key, value):
        self.values[key] = value


def test_worker_writes_latest_order_status_to_redis():
    order_id = UUID("016d84d5-79f8-41a3-a6c3-d8da55b4b581")
    event = OrderEvent(
        order_id=order_id,
        status=OrderStatus.SHIPPED,
        occurred_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    redis_client = FakeRedis()

    asyncio.run(process_event(event.model_dump_json().encode(), redis_client))

    saved = json.loads(redis_client.values[order_status_key(order_id)])
    assert saved["order_id"] == str(order_id)
    assert saved["status"] == "shipped"
    assert saved["updated_at"] == "2026-01-02T00:00:00Z"


def test_worker_rejects_malformed_event():
    redis_client = FakeRedis()

    with pytest.raises(ValueError):
        asyncio.run(process_event(b"not-json", redis_client))
    assert redis_client.values == {}
