from datetime import timezone

from backend.streaming.bytewax_pipeline import (
    add_temperature,
    build_average_result,
    filter_positive_temperature,
    map_telemetry,
    merge_temperature,
    parse_timestamp,
)


def test_filter_accepts_positive_temperature():
    assert filter_positive_temperature(
        {
            "truck_id": "Truck-001",
            "temperature": 25.0,
            "timestamp": "2026-09-10T10:00:00Z",
        }
    )


def test_filter_rejects_zero_temperature():
    assert not filter_positive_temperature(
        {
            "truck_id": "Truck-001",
            "temperature": 0.0,
            "timestamp": "2026-09-10T10:00:00Z",
        }
    )


def test_filter_rejects_negative_temperature():
    assert not filter_positive_temperature(
        {
            "truck_id": "Truck-001",
            "temperature": -5.0,
            "timestamp": "2026-09-10T10:00:00Z",
        }
    )


def test_timestamp_is_converted_to_utc():
    timestamp = parse_timestamp(
        "2026-09-10T10:00:00Z"
    )

    assert timestamp.tzinfo == timezone.utc


def test_map_normalizes_temperature():
    result = map_telemetry(
        {
            "truck_id": "Truck-001",
            "temperature": 25.678,
            "timestamp": "2026-09-10T10:00:00Z",
        }
    )

    assert result["truck_id"] == "Truck-001"
    assert result["temperature"] == 25.68


def test_window_average_accumulator():
    accumulator = {
        "temperature_sum": 0.0,
        "event_count": 0,
    }

    accumulator = add_temperature(
        accumulator,
        {
            "truck_id": "Truck-001",
            "temperature": 30.0,
        },
    )

    accumulator = add_temperature(
        accumulator,
        {
            "truck_id": "Truck-001",
            "temperature": 35.0,
        },
    )

    assert accumulator["temperature_sum"] == 65.0
    assert accumulator["event_count"] == 2


def test_window_average_result():
    result = build_average_result(
        (
            "Truck-001",
            (
                0,
                {
                    "temperature_sum": 65.0,
                    "event_count": 2,
                },
            ),
        )
    )

    assert result["truck_id"] == "Truck-001"
    assert result["event_count"] == 2
    assert result["average_temperature"] == 32.5


def test_window_accumulators_merge():
    left = {
        "temperature_sum": 30.0,
        "event_count": 1,
    }

    right = {
        "temperature_sum": 70.0,
        "event_count": 2,
    }

    result = merge_temperature(left, right)

    assert result == {
        "temperature_sum": 100.0,
        "event_count": 3,
    }
