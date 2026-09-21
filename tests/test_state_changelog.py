import json
import uuid

from confluent_kafka import Consumer

from backend.kafka.config import KAFKA_BOOTSTRAP_SERVERS
from backend.kafka.state_changelog import StateChangelogProducer
from backend.kafka.topics import STATE_CHANGELOG_TOPIC


def test_state_changelog_publish():
    group_id = (
        f"streamforge-state-test-{uuid.uuid4().hex[:8]}"
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "latest",
        }
    )

    producer = StateChangelogProducer()

    try:
        consumer.subscribe([STATE_CHANGELOG_TOPIC])

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

        test_key = (
            f"truck:TEST-{uuid.uuid4().hex[:8]}"
        )

        test_state = {
            "truck_id": "TEST-000001",
            "temperature": 27.5,
            "timestamp": "2026-09-21T12:00:00+00:00",
        }

        producer.publish(
            test_key,
            test_state,
        )

        producer.flush()

        message = None

        for _ in range(20):
            candidate = consumer.poll(0.5)

            if candidate is None:
                continue

            if candidate.error():
                continue

            candidate_key = candidate.key()

            if isinstance(candidate_key, bytes):
                candidate_key = candidate_key.decode("utf-8")

            if candidate_key == test_key:
                message = candidate
                break

        assert message is not None

        assert message.topic() == STATE_CHANGELOG_TOPIC

        message_key = message.key()

        if isinstance(message_key, bytes):
            message_key = message_key.decode("utf-8")

        assert message_key == test_key

        payload = json.loads(
            message.value().decode("utf-8")
        )

        assert payload == test_state

    finally:
        consumer.close()
