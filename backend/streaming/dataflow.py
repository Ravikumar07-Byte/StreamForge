"""Telemetry streaming dataflow utilities."""

from datetime import datetime

from backend.models.telemetry import Telemetry
from backend.streaming.transformations import transform_telemetry
from backend.streaming.windowing import (
    calculate_five_minute_averages,
    separate_late_events,
)


def process_telemetry(telemetry: Telemetry) -> Telemetry | None:
    """Process one telemetry event through the streaming pipeline."""
    return transform_telemetry(telemetry)


def process_batch(
    telemetry_events: list[Telemetry],
) -> list[Telemetry]:
    """Process a batch of telemetry events."""
    processed: list[Telemetry] = []

    for telemetry in telemetry_events:
        result = process_telemetry(telemetry)

        if result is not None:
            processed.append(result)

    return processed


def process_five_minute_window(
    telemetry_events: list[Telemetry],
    current_watermark: datetime | None = None,
    allowed_lateness_seconds: int = 60,
) -> tuple[list[dict[str, object]], list[Telemetry]]:
    """Process telemetry into five-minute averages and identify late events.

    Events are filtered and transformed before aggregation. When a watermark
    is supplied, events beyond the allowed lateness threshold are separated
    from the events used for the current aggregation.
    """
    processed = process_batch(telemetry_events)

    if current_watermark is None:
        on_time = processed
        late: list[Telemetry] = []
    else:
        on_time, late = separate_late_events(
            processed,
            current_watermark,
            allowed_lateness_seconds,
        )

    averages = calculate_five_minute_averages(on_time)

    return averages, late
