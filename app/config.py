import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "order-events"
    kafka_group_id: str = "order-status-worker"
    redis_url: str = "redis://redis:6379/0"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers
            ),
            kafka_topic=os.getenv("KAFKA_TOPIC", cls.kafka_topic),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", cls.kafka_group_id),
            redis_url=os.getenv("REDIS_URL", cls.redis_url),
        )
