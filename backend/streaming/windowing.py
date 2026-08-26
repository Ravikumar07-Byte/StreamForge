"""Telemetry event-time windowing utilities."""

from collections import defaultdict
from datetime import datetime, timedelta

from backend.models.telemetry import Telemetry


def group_by_truck(
    telemetry_events: list[Telemetry],
) -> dict[str, list[Telemetry]]:
    """Group telemetry events by truck ID."""
    grouped: dict[str, list[Telemetry]] = defaultdict(list)

    for telemetry in telemetry_events:
        grouped[telemetry.truck_id].append(telemetry)

    return dict(grouped)


def group_by_minute(
    telemetry_events: list[Telemetry],
) -> dict[datetime, list[Telemetry]]:
    """Group telemetry events into one-minute event-time windows."""
    grouped: dict[datetime, list[Telemetry]] = defaultdict(list)

    for telemetry in telemetry_events:
        window_start = telemetry.timestamp.replace(
            second=0,
            microsecond=0,
        )
        grouped[window_start].append(telemetry)

    return dict(grouped)


def get_five_minute_window_start(timestamp: datetime) -> datetime:
    """Return the start of the five-minute event-time window."""
    minute = (timestamp.minute // 5) * 5

    return timestamp.replace(
        minute=minute,
        second=0,
        microsecond=0,
    )


def group_by_truck_and_five_minutes(
    telemetry_events: list[Telemetry],
) -> dict[tuple[str, datetime], list[Telemetry]]:
    """Group telemetry by truck ID and five-minute event-time window."""
    grouped: dict[tuple[str, datetime], list[Telemetry]] = defaultdict(list)

    for telemetry in telemetry_events:
        window_start = get_five_minute_window_start(
            telemetry.timestamp
        )

        key = (telemetry.truck_id, window_start)
        grouped[key].append(telemetry)

    return dict(grouped)


def calculate_five_minute_averages(
    telemetry_events: list[Telemetry],
) -> list[dict[str, object]]:
    """Calculate average temperature for each truck's five-minute window."""
    grouped = group_by_truck_and_five_minutes(telemetry_events)

    results: list[dict[str, object]] = []

    for (truck_id, window_start), events in sorted(grouped.items()):
        temperatures = [event.temperature for event in events]

        average_temperature = sum(temperatures) / len(temperatures)

        results.append(
            {
                "truck_id": truck_id,
                "window_start": window_start,
                "window_end": window_start + timedelta(minutes=5),
                "temperature_average": round(
                    average_temperature,
                    2,
                ),
                "event_count": len(events),
            }
        )

    return results


def is_late_event(
    event_timestamp: datetime,
    current_watermark: datetime,
    allowed_lateness_seconds: int = 60,
) -> bool:
    """Return True when an event arrives beyond the allowed lateness."""
    return event_timestamp < (
        current_watermark - timedelta(seconds=allowed_lateness_seconds)
    )


def separate_late_events(
    telemetry_events: list[Telemetry],
    current_watermark: datetime,
    allowed_lateness_seconds: int = 60,
) -> tuple[list[Telemetry], list[Telemetry]]:
    """Separate on-time events from events beyond the lateness threshold."""
    on_time: list[Telemetry] = []
    late: list[Telemetry] = []

    for telemetry in telemetry_events:
        if is_late_event(
            telemetry.timestamp,
            current_watermark,
            allowed_lateness_seconds,
        ):
            late.append(telemetry)
        else:
            on_time.append(telemetry)

    return on_time, late
