import json
import uuid

from confluent_kafka import Consumer

from backend.kafka.config import KAFKA_BOOTSTRAP_SERVERS
from backend.kafka.topics import STATE_CHANGELOG_TOPIC
from backend.state.manager import PersistentStateManager


def test_persistent_state_manager():
    db_path = (
        f"data/test_state_manager_"
        f"{uuid.uuid4().hex[:8]}"
    )

    manager = PersistentStateManager(
        db_path=db_path,
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

    consumer = None

    try:
        # ---------------------------------------------------------
        # 1. Persist state and publish changelog.
        # ---------------------------------------------------------
        manager.put(
            key=key,
            value=state,
        )

        manager.flush()

        # ---------------------------------------------------------
        # 2. Verify local RocksDB state.
        # ---------------------------------------------------------
        assert manager.exists(key)

        stored_state = manager.get(key)

        assert stored_state == state

        # ---------------------------------------------------------
        # 3. Create a new consumer.
        #
        # Since this is a unique consumer group and
        # auto.offset.reset is "earliest", Kafka will read
        # existing records from the beginning of the topic.
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

        consumer.subscribe(
            [STATE_CHANGELOG_TOPIC]
        )

        # ---------------------------------------------------------
        # 4. Wait for partition assignment.
        # ---------------------------------------------------------
        assignment = []

        for _ in range(20):
            consumer.poll(0.5)

            assignment = consumer.assignment()

            if assignment:
                break

        assert assignment, (
            "Kafka state changelog consumer "
            "was not assigned a partition"
        )

        # ---------------------------------------------------------
        # 5. Read records until our unique key appears.
        # ---------------------------------------------------------
        message = None

        for _ in range(40):
            candidate = consumer.poll(0.5)

            if candidate is None:
                continue

            if candidate.error():
                continue

            if candidate.topic() != STATE_CHANGELOG_TOPIC:
                continue

            # Kafka returns keys as bytes.
            candidate_key = candidate.key()

            if candidate_key is None:
                continue

            candidate_key = candidate_key.decode("utf-8")

            if candidate_key != key:
                continue

            message = candidate
            break

        assert message is not None, (
            "State changelog record was not received "
            "by the consumer"
        )

        # ---------------------------------------------------------
        # 6. Verify Kafka record.
        # ---------------------------------------------------------
        assert (
            message.topic()
            == STATE_CHANGELOG_TOPIC
        )

        assert message.key().decode("utf-8") == key

        payload = json.loads(
            message.value().decode("utf-8")
        )

        assert payload == state

    finally:
        manager.close()

        if consumer is not None:
            consumer.close()
