from datetime import datetime

from backend.models.telemetry import Telemetry


def test_telemetry_creation():
    telemetry = Telemetry(
        truck_id="TRUCK-000001",
        temperature=32.5,
    )

    assert telemetry.truck_id == "TRUCK-000001"
    assert telemetry.temperature == 32.5
    assert isinstance(telemetry.timestamp, datetime)


def test_telemetry_json_serialization():
    telemetry = Telemetry(
        truck_id="TRUCK-000001",
        temperature=28.7,
    )

    payload = telemetry.model_dump_json()

    assert "TRUCK-000001" in payload
    assert "28.7" in payload


def test_empty_truck_id_is_rejected():
    try:
        Telemetry(
            truck_id="",
            temperature=25.0,
        )
        assert False
    except ValueError:
        assert True
