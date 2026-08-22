"""End-to-end Kafka telemetry pipeline test."""

import time

from backend.kafka.consumer import TelemetryConsumer
from backend.kafka.producer import TelemetryProducer
from backend.models.telemetry import Telemetry


def test_telemetry_producer_consumer_flow():
    group_id = "streamforge-day7-integration"

    consumer = TelemetryConsumer(
        group_id=group_id,
        auto_offset_reset="latest",
    )

    producer = TelemetryProducer()

    try:
        # Give Kafka time to establish the consumer assignment.
        for _ in range(10):
            consumer.consumer.poll(0.5)

            if consumer.consumer.assignment():
                break

        assert consumer.consumer.assignment(), (
            "Kafka consumer was not assigned any partitions"
        )

        telemetry = Telemetry(
            truck_id="TRUCK-DAY7-INTEGRATION",
            temperature=28.7,
        )

        producer.publish(telemetry)
        producer.flush()

        received = None

        for _ in range(10):
            received = consumer.consume_one(timeout=2.0)

            if received is not None:
                if received.truck_id == telemetry.truck_id:
                    break

        assert received is not None
        assert received.truck_id == telemetry.truck_id
        assert received.temperature == telemetry.temperature
        assert received.timestamp is not None

    finally:
        consumer.close()
