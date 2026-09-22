"""Restore persistent state from the Kafka state changelog."""

import json

from confluent_kafka import Consumer, TopicPartition

from backend.kafka.config import KAFKA_BOOTSTRAP_SERVERS
from backend.kafka.topics import STATE_CHANGELOG_TOPIC
from backend.state.rocksdb_store import RocksDBStore


def restore_from_changelog(
    store: RocksDBStore,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = STATE_CHANGELOG_TOPIC,
    timeout: float = 10.0,
) -> int:
    """Replay Kafka state changelog records into RocksDB.

    Records are replayed from the beginning of each partition.
    When the same key appears multiple times, the latest record
    encountered in the replay becomes the restored state.

    Returns:
        Number of state records restored.
    """

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "streamforge-state-recovery",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )

    restored = 0

    try:
        metadata = consumer.list_topics(
            topic=topic,
            timeout=timeout,
        )

        topic_metadata = metadata.topics.get(topic)

        if topic_metadata is None:
            raise RuntimeError(
                f"Kafka topic does not exist: {topic}"
            )

        if topic_metadata.error is not None:
            raise RuntimeError(
                f"Unable to read Kafka topic metadata: "
                f"{topic_metadata.error}"
            )

        partitions = sorted(
            topic_metadata.partitions.keys()
        )

        if not partitions:
            return 0

        assignments = []
        end_offsets = {}

        for partition in partitions:
            topic_partition = TopicPartition(
                topic,
                partition,
            )

            low, high = consumer.get_watermark_offsets(
                topic_partition,
                timeout=timeout,
            )

            if high <= low:
                continue

            assignments.append(
                TopicPartition(
                    topic,
                    partition,
                    low,
                )
            )

            end_offsets[partition] = high

        if not assignments:
            return 0

        consumer.assign(assignments)

        completed_partitions = set()

        while len(completed_partitions) < len(
            end_offsets
        ):
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise RuntimeError(
                    f"Kafka changelog consumer error: "
                    f"{message.error()}"
                )

            partition = message.partition()

            if partition in completed_partitions:
                continue

            if message.key() is None:
                continue

            key = message.key().decode("utf-8")

            if message.value() is None:
                store.delete(key)
            else:
                value = json.loads(
                    message.value().decode("utf-8")
                )

                store.put(
                    key,
                    value,
                )

                restored += 1

            if (
                message.offset() + 1
                >= end_offsets[partition]
            ):
                completed_partitions.add(partition)

        return restored

    finally:
        consumer.close()