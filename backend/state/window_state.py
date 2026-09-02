"""Persistent five-minute window state for StreamForge."""

from datetime import datetime

from backend.state.rocksdb_store import RocksDBStore


WINDOW_PREFIX = "window:"


def _window_key(truck_id: str, window_start: datetime) -> str:
    """Build the RocksDB key for a truck's five-minute window."""
    return f"{WINDOW_PREFIX}{truck_id}:{window_start.isoformat()}"


def save_window_event(
    store: RocksDBStore,
    truck_id: str,
    window_start: datetime,
    temperature: float,
) -> dict[str, object]:
    """Add one telemetry event to a persisted five-minute window."""
    key = _window_key(truck_id, window_start)

    existing = store.get(key)

    if isinstance(existing, dict):
        event_count = int(existing.get("event_count", 0))
        temperature_sum = float(existing.get("temperature_sum", 0.0))
    else:
        event_count = 0
        temperature_sum = 0.0

    event_count += 1
    temperature_sum += temperature

    state: dict[str, object] = {
        "truck_id": truck_id,
        "window_start": window_start.isoformat(),
        "event_count": event_count,
        "temperature_sum": temperature_sum,
        "temperature_average": round(
            temperature_sum / event_count,
            2,
        ),
    }

    store.put(key, state)

    return state


def load_window_state(
    store: RocksDBStore,
    truck_id: str,
    window_start: datetime,
) -> dict[str, object] | None:
    """Load one persisted five-minute window."""
    value = store.get(_window_key(truck_id, window_start))

    if not isinstance(value, dict):
        return None

    return value


def list_window_states(
    store: RocksDBStore,
) -> list[dict[str, object]]:
    """Return all persisted five-minute window states."""
    states: list[dict[str, object]] = []

    for key in store.keys():
        if not key.startswith(WINDOW_PREFIX):
            continue

        value = store.get(key)

        if isinstance(value, dict):
            states.append(value)

    return states
