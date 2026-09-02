"""End-to-end Kafka telemetry producer/consumer integration test."""

import time
import uuid

from backend.kafka.consumer import TelemetryConsumer
from backend.kafka.producer import TelemetryProducer
from backend.models.telemetry import Telemetry


def test_telemetry_producer_consumer_flow():
    """Verify telemetry can be published to Kafka and consumed successfully."""

    group_id = f"streamforge-e2e-{uuid.uuid4().hex[:8]}"

    consumer = TelemetryConsumer(
        group_id=group_id,
        auto_offset_reset="latest",
    )

    producer = TelemetryProducer()

    try:
        # Allow Kafka enough time to establish the consumer assignment.
        assignment_timeout = 15.0
        start_time = time.monotonic()

        while time.monotonic() - start_time < assignment_timeout:
            consumer.consumer.poll(0.5)

            if consumer.consumer.assignment():
                break

        assert consumer.consumer.assignment(), (
            "Kafka consumer was not assigned any partitions "
            f"within {assignment_timeout} seconds"
        )

        telemetry = Telemetry(
            truck_id="TRUCK-DAY7-INTEGRATION",
            temperature=28.7,
        )

        producer.publish(telemetry)
        producer.flush()

        received = None
        receive_timeout = 15.0
        start_time = time.monotonic()

        while time.monotonic() - start_time < receive_timeout:
            message = consumer.consume_one(timeout=1.0)

            if message is None:
                continue

            if message.truck_id == telemetry.truck_id:
                received = message
                break

        assert received is not None, (
            "Telemetry message was not received from Kafka "
            f"within {receive_timeout} seconds"
        )

        assert received.truck_id == telemetry.truck_id
        assert received.temperature == telemetry.temperature
        assert received.timestamp is not None

    finally:
        consumer.close()