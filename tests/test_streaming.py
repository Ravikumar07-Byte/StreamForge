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

def test_five_minute_window_start():
    telemetry = Telemetry(
        truck_id="TRUCK-001",
        temperature=25.0,
        timestamp=datetime(
            2026,
            8,
            24,
            12,
            34,
            30,
            tzinfo=timezone.utc,
        ),
    )

    from backend.streaming.windowing import get_five_minute_window_start

    window_start = get_five_minute_window_start(telemetry.timestamp)

    assert window_start == datetime(
        2026,
        8,
        24,
        12,
        30,
        0,
        tzinfo=timezone.utc,
    )


def test_group_by_truck_and_five_minutes():
    from backend.streaming.windowing import group_by_truck_and_five_minutes

    events = [
        Telemetry(
            truck_id="TRUCK-001",
            temperature=20.0,
            timestamp=datetime(
                2026,
                8,
                24,
                12,
                30,
                10,
                tzinfo=timezone.utc,
            ),
        ),
        Telemetry(
            truck_id="TRUCK-001",
            temperature=22.0,
            timestamp=datetime(
                2026,
                8,
                24,
                12,
                34,
                50,
                tzinfo=timezone.utc,
            ),
        ),
        Telemetry(
            truck_id="TRUCK-001",
            temperature=30.0,
            timestamp=datetime(
                2026,
                8,
                24,
                12,
                35,
                5,
                tzinfo=timezone.utc,
            ),
        ),
    ]

    grouped = group_by_truck_and_five_minutes(events)

    assert len(grouped) == 2

    first_window = (
        "TRUCK-001",
        datetime(
            2026,
            8,
            24,
            12,
            30,
            0,
            tzinfo=timezone.utc,
        ),
    )

    second_window = (
        "TRUCK-001",
        datetime(
            2026,
            8,
            24,
            12,
            35,
            0,
            tzinfo=timezone.utc,
        ),
    )

    assert len(grouped[first_window]) == 2
    assert len(grouped[second_window]) == 1


def test_calculate_five_minute_temperature_average():
    from backend.streaming.windowing import calculate_five_minute_averages

    events = [
        Telemetry(
            truck_id="TRUCK-001",
            temperature=20.0,
            timestamp=datetime(
                2026,
                8,
                24,
                12,
                30,
                10,
                tzinfo=timezone.utc,
            ),
        ),
        Telemetry(
            truck_id="TRUCK-001",
            temperature=22.0,
            timestamp=datetime(
                2026,
                8,
                24,
                12,
                32,
                20,
                tzinfo=timezone.utc,
            ),
        ),
        Telemetry(
            truck_id="TRUCK-001",
            temperature=24.0,
            timestamp=datetime(
                2026,
                8,
                24,
                12,
                34,
                50,
                tzinfo=timezone.utc,
            ),
        ),
    ]

    results = calculate_five_minute_averages(events)

    assert len(results) == 1

    result = results[0]

    assert result["truck_id"] == "TRUCK-001"
    assert result["temperature_average"] == 22.0
    assert result["event_count"] == 3
    assert result["window_start"] == datetime(
        2026,
        8,
        24,
        12,
        30,
        0,
        tzinfo=timezone.utc,
    )
    assert result["window_end"] == datetime(
        2026,
        8,
        24,
        12,
        35,
        0,
        tzinfo=timezone.utc,
    )


def test_five_minute_window_handles_hour_boundary():
    from backend.streaming.windowing import calculate_five_minute_averages

    event = Telemetry(
        truck_id="TRUCK-001",
        temperature=70.0,
        timestamp=datetime(
            2026,
            8,
            24,
            12,
            59,
            30,
            tzinfo=timezone.utc,
        ),
    )

    results = calculate_five_minute_averages([event])

    assert results[0]["window_start"] == datetime(
        2026,
        8,
        24,
        12,
        55,
        0,
        tzinfo=timezone.utc,
    )

    assert results[0]["window_end"] == datetime(
        2026,
        8,
        24,
        13,
        0,
        0,
        tzinfo=timezone.utc,
    )

def test_late_event_detection():
    from backend.streaming.windowing import is_late_event

    watermark = datetime(
        2026,
        8,
        24,
        12,
        35,
        0,
        tzinfo=timezone.utc,
    )

    on_time_event = datetime(
        2026,
        8,
        24,
        12,
        34,
        0,
        tzinfo=timezone.utc,
    )

    late_event = datetime(
        2026,
        8,
        24,
        12,
        33,
        0,
        tzinfo=timezone.utc,
    )

    assert is_late_event(
        on_time_event,
        watermark,
        allowed_lateness_seconds=60,
    ) is False

    assert is_late_event(
        late_event,
        watermark,
        allowed_lateness_seconds=60,
    ) is True


def test_separate_late_events():
    from backend.streaming.windowing import separate_late_events

    events = [
        Telemetry(
            truck_id="TRUCK-001",
            temperature=25.0,
            timestamp=datetime(
                2026,
                8,
                24,
                12,
                34,
                30,
                tzinfo=timezone.utc,
            ),
        ),
        Telemetry(
            truck_id="TRUCK-001",
            temperature=26.0,
            timestamp=datetime(
                2026,
                8,
                24,
                12,
                33,
                0,
                tzinfo=timezone.utc,
            ),
        ),
    ]

    watermark = datetime(
        2026,
        8,
        24,
        12,
        35,
        0,
        tzinfo=timezone.utc,
    )

    on_time, late = separate_late_events(
        events,
        watermark,
        allowed_lateness_seconds=60,
    )

    assert len(on_time) == 1
    assert len(late) == 1

    assert on_time[0].temperature == 25.0
    assert late[0].temperature == 26.0


def test_late_event_still_belongs_to_original_event_time_window():
    from backend.streaming.windowing import (
        get_five_minute_window_start,
    )

    late_event_timestamp = datetime(
        2026,
        8,
        24,
        12,
        33,
        0,
        tzinfo=timezone.utc,
    )

    window_start = get_five_minute_window_start(
        late_event_timestamp
    )

    assert window_start == datetime(
        2026,
        8,
        24,
        12,
        30,
        0,
        tzinfo=timezone.utc,
    )
