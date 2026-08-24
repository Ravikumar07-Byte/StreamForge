"""Telemetry streaming dataflow utilities."""

from backend.models.telemetry import Telemetry
from backend.streaming.transformations import transform_telemetry


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
