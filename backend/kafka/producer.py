"""Kafka producer for StreamForge truck telemetry."""

from confluent_kafka import Producer

from backend.kafka.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_PRODUCER_CLIENT_ID,
)
from backend.kafka.topics import TRUCK_TELEMETRY_TOPIC
from backend.models.telemetry import Telemetry


class TelemetryProducer:
    """Publish truck telemetry events to Kafka."""

    def __init__(self) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "client.id": KAFKA_PRODUCER_CLIENT_ID,
            }
        )

    def publish(self, telemetry: Telemetry) -> None:
        """Publish one telemetry event to Kafka."""

        self.producer.produce(
            topic=TRUCK_TELEMETRY_TOPIC,
            key=telemetry.truck_id,
            value=telemetry.model_dump_json(),
            callback=self._delivery_report,
        )

        self.producer.poll(0)

    def publish_batch(self, telemetry_events: list[Telemetry]) -> None:
        """Publish multiple telemetry events efficiently."""

        for telemetry in telemetry_events:
            self.producer.produce(
                topic=TRUCK_TELEMETRY_TOPIC,
                key=telemetry.truck_id,
                value=telemetry.model_dump_json(),
                callback=self._delivery_report,
            )

        self.producer.flush()

    def flush(self) -> None:
        """Wait for pending Kafka messages to be delivered."""

        self.producer.flush()

    @staticmethod
    def _delivery_report(err, msg) -> None:
        """Handle Kafka message delivery result."""

        if err is not None:
            print(f"Kafka delivery failed: {err}")
            return

        print(
            f"Kafka message delivered: "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )
