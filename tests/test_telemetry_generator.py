from backend.producers.telemetry_generator import (
    generate_telemetry,
    generate_truck_ids,
)


def test_generate_telemetry():
    telemetry = generate_telemetry("TRUCK-000001")

    assert telemetry.truck_id == "TRUCK-000001"
    assert 15.0 <= telemetry.temperature <= 45.0
    assert telemetry.timestamp is not None


def test_generate_truck_ids():
    truck_ids = generate_truck_ids(3)

    assert truck_ids == [
        "TRUCK-000001",
        "TRUCK-000002",
        "TRUCK-000003",
    ]
