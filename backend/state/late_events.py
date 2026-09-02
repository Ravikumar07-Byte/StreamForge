"""Persistent late-event state for StreamForge."""

from datetime import datetime

from backend.models.telemetry import Telemetry
from backend.state.rocksdb_store import RocksDBStore


LATE_EVENT_PREFIX = "late:"


def _late_event_key(telemetry: Telemetry) -> str:
    """Build a unique key for a late telemetry event."""
    return (
        f"{LATE_EVENT_PREFIX}"
        f"{telemetry.truck_id}:"
        f"{telemetry.timestamp.isoformat()}"
    )


def save_late_event(
    store: RocksDBStore,
    telemetry: Telemetry,
    watermark: datetime,
) -> dict[str, object]:
    """Persist a telemetry event identified as late."""
    state: dict[str, object] = {
        "truck_id": telemetry.truck_id,
        "temperature": telemetry.temperature,
        "timestamp": telemetry.timestamp.isoformat(),
        "watermark": watermark.isoformat(),
    }

    store.put(
        _late_event_key(telemetry),
        state,
    )

    return state


def list_late_events(
    store: RocksDBStore,
) -> list[dict[str, object]]:
    """Return all persisted late telemetry events."""
    events: list[dict[str, object]] = []

    for key in store.keys():
        if not key.startswith(LATE_EVENT_PREFIX):
            continue

        value = store.get(key)

        if isinstance(value, dict):
            events.append(value)

    return events
