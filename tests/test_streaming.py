"""Tests for StreamForge telemetry streaming processing."""

from datetime import datetime, timezone

from backend.models.telemetry import Telemetry
from backend.streaming.dataflow import process_batch, process_telemetry
from backend.streaming.filters import filter_telemetry, is_valid_temperature
from backend.streaming.transformations import normalize_temperature
from backend.streaming.windowing import group_by_minute, group_by_truck


def make_telemetry(
    truck_id: str = "TRUCK-001",
    temperature: float = 25.0,
    second: int = 10,
) -> Telemetry:
    return Telemetry(
        truck_id=truck_id,
        temperature=temperature,
        timestamp=datetime(
            2026,
            8,
            24,
            12,
            30,
            second,
            tzinfo=timezone.utc,
        ),
    )


def test_valid_temperature():
    telemetry = make_telemetry(temperature=25.0)

    assert is_valid_temperature(telemetry) is True


def test_invalid_temperature():
    telemetry = make_telemetry(temperature=150.0)

    assert is_valid_temperature(telemetry) is False
    assert filter_telemetry(telemetry) is None


def test_temperature_transformation():
    telemetry = make_telemetry(temperature=25.678)

    transformed = normalize_temperature(telemetry)

    assert transformed.temperature == 25.68


def test_process_telemetry():
    telemetry = make_telemetry(temperature=25.678)

    processed = process_telemetry(telemetry)

    assert processed is not None
    assert processed.temperature == 25.68


def test_invalid_telemetry_is_removed_from_batch():
    events = [
        make_telemetry("TRUCK-001", 25.0),
        make_telemetry("TRUCK-002", 150.0),
    ]

    processed = process_batch(events)

    assert len(processed) == 1
    assert processed[0].truck_id == "TRUCK-001"


def test_group_by_truck():
    events = [
        make_telemetry("TRUCK-001", 25.0),
        make_telemetry("TRUCK-001", 26.0),
        make_telemetry("TRUCK-002", 30.0),
    ]

    grouped = group_by_truck(events)

    assert len(grouped["TRUCK-001"]) == 2
    assert len(grouped["TRUCK-002"]) == 1


def test_group_by_minute():
    events = [
        make_telemetry("TRUCK-001", 25.0, 10),
        make_telemetry("TRUCK-001", 26.0, 45),
    ]

    grouped = group_by_minute(events)

    assert len(grouped) == 1
    assert len(next(iter(grouped.values()))) == 2
