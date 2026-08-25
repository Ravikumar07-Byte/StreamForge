"""Telemetry API routes."""

from collections import deque
from threading import Lock

from backend.models.telemetry import Telemetry


# Store the most recent telemetry events in memory.
_MAX_EVENTS = 100
_telemetry_buffer = deque(maxlen=_MAX_EVENTS)
_buffer_lock = Lock()


def add_telemetry(telemetry: Telemetry) -> None:
    """Add a telemetry event to the recent-events buffer."""
    with _buffer_lock:
        _telemetry_buffer.append(telemetry)


def get_telemetry() -> list[Telemetry]:
    """Return the most recent telemetry events."""
    with _buffer_lock:
        return list(_telemetry_buffer)