"""Kafka consumer for StreamForge truck telemetry."""

import json

from confluent_kafka import Consumer, KafkaException

from backend.kafka.config import KAFKA_BOOTSTRAP_SERVERS
from backend.kafka.topics import TRUCK_TELEMETRY_TOPIC
from backend.models.telemetry import Telemetry


class TelemetryConsumer:
    """Consume truck telemetry events from Kafka."""

    def __init__(
        self,
        group_id: str = "streamforge-telemetry-consumer",
        auto_offset_reset: str = "earliest",
    ) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": True,
            }
        )

        self.consumer.subscribe([TRUCK_TELEMETRY_TOPIC])

    def consume_one(self, timeout: float = 5.0) -> Telemetry | None:
        """Consume and validate one telemetry event."""

        message = self.consumer.poll(timeout)

        if message is None:
            return None

        if message.error():
            raise KafkaException(message.error())

        payload = json.loads(message.value().decode("utf-8"))

        return Telemetry.model_validate(payload)

    def close(self) -> None:
        """Close the Kafka consumer."""

        self.consumer.close()
