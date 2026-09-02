"""Telemetry API routes."""

from collections import deque
from threading import Lock

from backend.models.telemetry import Telemetry
from backend.state.rocksdb_store import RocksDBStore
from backend.state.truck_state import get_active_trucks


# Store the most recent telemetry events in memory.
# This remains useful when the consumer and API run in
# the same process, but persistent state is used by the API
# for cross-process dashboard access.
_MAX_EVENTS = 100
_telemetry_buffer = deque(maxlen=_MAX_EVENTS)
_buffer_lock = Lock()

STATE_PATH = "data/state"


def add_telemetry(telemetry: Telemetry) -> None:
    """Add a telemetry event to the recent-events buffer."""

    with _buffer_lock:
        _telemetry_buffer.append(telemetry)


def get_telemetry() -> list[Telemetry]:
    """Return the most recent telemetry events."""

    with _buffer_lock:
        return list(_telemetry_buffer)


def get_persisted_truck_states() -> list[dict[str, object]]:
    """Return the latest persisted state for every truck."""

    store = RocksDBStore(STATE_PATH)

    try:
        states: list[dict[str, object]] = []

        for key in store.keys():
            if not key.startswith("truck:"):
                continue

            truck_id = key[len("truck:"):]

            if not truck_id:
                continue

            state = store.get(key)

            if isinstance(state, dict):
                states.append(state)

        return states

    finally:
        store.close()


def get_persisted_active_trucks() -> set[str]:
    """Return trucks currently active according to persisted state."""

    store = RocksDBStore(STATE_PATH)

    try:
        return get_active_trucks(store)
    finally:
        store.close()