import json
import uuid

from confluent_kafka import Consumer, TopicPartition

from backend.kafka.config import KAFKA_BOOTSTRAP_SERVERS
from backend.kafka.topics import STATE_CHANGELOG_TOPIC
from backend.state.manager import PersistentStateManager


def test_persistent_state_manager():
    db_path = (
        f"data/test_state_manager_"
        f"{uuid.uuid4().hex[:8]}"
    )

    key = (
        f"truck:TEST-{uuid.uuid4().hex[:8]}"
    )

    state = {
        "truck_id": "TEST-000001",
        "temperature": 28.5,
        "speed": 62.0,
        "status": "active",
    }

    manager = PersistentStateManager(
        db_path=db_path,
    )

    consumer = None

    try:
        # ---------------------------------------------------------
        # 1. Create a dedicated Kafka consumer.
        # ---------------------------------------------------------
        group_id = (
            f"streamforge-manager-test-"
            f"{uuid.uuid4().hex[:8]}"
        )

        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        # ---------------------------------------------------------
        # 2. Discover all partitions.
        # ---------------------------------------------------------
        metadata = consumer.list_topics(
            topic=STATE_CHANGELOG_TOPIC,
            timeout=5.0,
        )

        topic_metadata = metadata.topics[
            STATE_CHANGELOG_TOPIC
        ]

        partitions = sorted(
            topic_metadata.partitions.keys()
        )

        assert partitions, (
            "State changelog topic has no partitions"
        )

        # ---------------------------------------------------------
        # 3. Capture the current end offset of every partition.
        #
        # The consumer will start AFTER the existing records.
        # This prevents the test from scanning old changelog data.
        # ---------------------------------------------------------
        starting_positions = []

        for partition_id in partitions:
            partition = TopicPartition(
                STATE_CHANGELOG_TOPIC,
                partition_id,
            )

            low_offset, high_offset = (
                consumer.get_watermark_offsets(
                    partition,
                    timeout=5.0,
                )
            )

            assert low_offset >= 0
            assert high_offset >= low_offset

            starting_positions.append(
                TopicPartition(
                    STATE_CHANGELOG_TOPIC,
                    partition_id,
                    high_offset,
                )
            )

        # ---------------------------------------------------------
        # 4. Explicitly assign partitions at their current end.
        #
        # This is deterministic and avoids subscribe/seek timing
        # races.
        # ---------------------------------------------------------
        consumer.assign(starting_positions)

        assert consumer.assignment()

        # ---------------------------------------------------------
        # 5. NOW publish the test state.
        # ---------------------------------------------------------
        manager.put(
            key=key,
            value=state,
        )

        manager.flush()

        # ---------------------------------------------------------
        # 6. Verify local RocksDB state.
        # ---------------------------------------------------------
        assert manager.exists(key)

        stored_state = manager.get(key)

        assert stored_state == state

        # ---------------------------------------------------------
        # 7. Read the newly produced Kafka record.
        # ---------------------------------------------------------
        message = None

        for _ in range(30):
            candidate = consumer.poll(1.0)

            if candidate is None:
                continue

            if candidate.error():
                continue

            if candidate.topic() != STATE_CHANGELOG_TOPIC:
                continue

            candidate_key = candidate.key()

            if candidate_key is None:
                continue

            if isinstance(candidate_key, bytes):
                candidate_key = candidate_key.decode(
                    "utf-8"
                )

            if candidate_key != key:
                continue

            message = candidate
            break

        assert message is not None, (
            "State changelog record was not received "
            "after deterministic assignment"
        )

        # ---------------------------------------------------------
        # 8. Verify Kafka record.
        # ---------------------------------------------------------
        assert (
            message.topic()
            == STATE_CHANGELOG_TOPIC
        )

        message_key = message.key()

        if isinstance(message_key, bytes):
            message_key = message_key.decode(
                "utf-8"
            )

        assert message_key == key

        payload = json.loads(
            message.value().decode("utf-8")
        )

        assert payload == state

    finally:
        manager.close()

        if consumer is not None:
            consumer.close()