"""Telemetry filtering utilities."""

from backend.models.telemetry import Telemetry


def is_valid_temperature(telemetry: Telemetry) -> bool:
    """Return True when telemetry contains a valid temperature."""
    return -50.0 <= telemetry.temperature <= 100.0


def filter_telemetry(telemetry: Telemetry) -> Telemetry | None:
    """Return telemetry when it passes validation."""
    if not is_valid_temperature(telemetry):
        return None

    return telemetry
