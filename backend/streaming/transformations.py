"""Telemetry transformation utilities."""

from backend.models.telemetry import Telemetry


def normalize_temperature(telemetry: Telemetry) -> Telemetry:
    """Return a telemetry object with normalized temperature precision."""
    return telemetry.model_copy(
        update={"temperature": round(telemetry.temperature, 2)}
    )


def transform_telemetry(telemetry: Telemetry) -> Telemetry | None:
    """Filter and transform a telemetry event."""
    from backend.streaming.filters import filter_telemetry

    filtered = filter_telemetry(telemetry)

    if filtered is None:
        return None

    return normalize_temperature(filtered)
