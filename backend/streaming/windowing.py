"""Simple telemetry windowing utilities."""

from collections import defaultdict
from datetime import datetime

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
    """Group telemetry events into one-minute windows."""
    grouped: dict[datetime, list[Telemetry]] = defaultdict(list)

    for telemetry in telemetry_events:
        window_start = telemetry.timestamp.replace(
            second=0,
            microsecond=0,
        )
        grouped[window_start].append(telemetry)

    return dict(grouped)
