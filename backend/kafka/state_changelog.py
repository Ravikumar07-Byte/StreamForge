"""Kafka producer for persistent StreamForge state changelog."""

import json

from confluent_kafka import Producer

from backend.kafka.config import (
    KAFKA_BOOTSTRAP_SERVERS,
)
from backend.kafka.topics import STATE_CHANGELOG_TOPIC


class StateChangelogProducer:
    """Publish persistent state changes to Kafka."""

    def __init__(self) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "streamforge-state-changelog-producer",
            }
        )

    def publish(
        self,
        key: str,
        state: object,
    ) -> None:
        """Publish one state snapshot to the changelog."""

        value = json.dumps(
            state,
            separators=(",", ":"),
        )

        self.producer.produce(
            topic=STATE_CHANGELOG_TOPIC,
            key=key,
            value=value,
            callback=self._delivery_report,
        )

        self.producer.poll(0)

    def flush(self) -> None:
        """Wait for pending changelog messages."""

        self.producer.flush()

    @staticmethod
    def _delivery_report(err, msg) -> None:
        """Handle changelog delivery result."""

        if err is not None:
            print(
                f"Kafka changelog delivery failed: {err}"
            )
            return

        print(
            "Kafka changelog delivered: "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )
