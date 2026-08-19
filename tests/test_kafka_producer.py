from backend.kafka.producer import TelemetryProducer
from backend.models.telemetry import Telemetry


def test_producer_can_be_created():
    producer = TelemetryProducer()

    assert producer is not None


def test_telemetry_can_be_published():
    producer = TelemetryProducer()

    telemetry = Telemetry(
        truck_id="TRUCK-000001",
        temperature=32.5,
    )

    producer.publish(telemetry)
    producer.flush()
