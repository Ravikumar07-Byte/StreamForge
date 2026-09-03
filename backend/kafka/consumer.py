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
                "enable.auto.commit": False,

                # Allow longer processing periods before Kafka
                # considers this consumer inactive.
                "max.poll.interval.ms": 900000,

                # Keep the consumer session alive while polling.
                "session.timeout.ms": 45000,
                "heartbeat.interval.ms": 15000,
            }
        )

        self.consumer.subscribe([TRUCK_TELEMETRY_TOPIC])

        # Kafka position of the most recently consumed message.
        self.last_partition: int | None = None
        self.last_offset: int | None = None

    def consume_one(
        self,
        timeout: float = 5.0,
    ) -> Telemetry | None:
        """Consume and validate one telemetry event."""

        message = self.consumer.poll(timeout)

        if message is None:
            return None

        if message.error():
            raise KafkaException(message.error())

        # Save the Kafka position before returning the telemetry.
        self.last_partition = message.partition()
        self.last_offset = message.offset()

        payload = json.loads(
            message.value().decode("utf-8")
        )

        return Telemetry.model_validate(payload)

    def commit(self) -> None:
        """Commit the latest consumed Kafka offset."""

        self.consumer.commit(
            asynchronous=False
        )

    def get_last_position(
        self,
    ) -> tuple[int, int] | None:
        """Return the partition and offset of the latest message."""

        if (
            self.last_partition is None
            or self.last_offset is None
        ):
            return None

        return (
            self.last_partition,
            self.last_offset,
        )

    def close(self) -> None:
        """Close the Kafka consumer."""

        self.consumer.close()