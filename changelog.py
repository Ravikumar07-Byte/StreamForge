"""
database/changelog.py
======================
Kafka-backed write-ahead log for the RocksDB state store.

This is what makes StreamForge's state recovery exactly-once-ish, matching
the use case: "If Worker Node #4 crashes, StreamForge ... recovers its
state from a RocksDB changelog."

The changelog topic should be created with `cleanup.policy=compact` so
Kafka itself keeps only the latest value per key indefinitely (like a
distributed, replicated backup of the RocksDB state), e.g.:

    kafka-topics --create --topic truck-state-changelog \\
        --config cleanup.policy=compact --partitions 20 --replication-factor 3
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterator

from confluent_kafka import Producer, Consumer, TopicPartition, KafkaException

from models.schemas import ChangelogRecord


class ChangelogWriter:
    """Appends every state mutation to the compacted changelog topic."""

    def __init__(self, topic: str, bootstrap_servers: str = "localhost:9092") -> None:
        self.topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                # Idempotent producer = no duplicate changelog entries on retry.
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def append(self, key: str, value: dict, op: str = "put") -> None:
        payload = {"key": key, "value": value, "op": op}
        self._producer.produce(
            topic=self.topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
        )
        # poll(0) drains delivery callbacks without blocking the caller.
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self.flush()


@dataclass
class ChangelogReplayer:
    """
    Reads the compacted changelog topic from the beginning (or from the
    last committed offset) to rebuild RocksDB state on a new/recovering
    worker.
    """

    topic: str
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "streamforge-changelog-replayer"

    def replay(self, timeout_seconds: float = 5.0) -> Iterator[ChangelogRecord]:
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        try:
            consumer.subscribe([self.topic])
            # Poll until no new message arrives within timeout -> topic
            # is considered "caught up" for this recovery pass.
            while True:
                msg = consumer.poll(timeout=timeout_seconds)
                if msg is None:
                    break
                if msg.error():
                    raise KafkaException(msg.error())

                payload = json.loads(msg.value().decode("utf-8"))
                yield ChangelogRecord(
                    key=payload["key"],
                    value=payload["value"],
                    op=payload.get("op", "put"),
                    offset=msg.offset(),
                )
        finally:
            consumer.close()
