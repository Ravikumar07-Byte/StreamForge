"""Persistent latest-state storage for truck telemetry."""

from datetime import datetime

from backend.models.telemetry import Telemetry
from backend.state.rocksdb_store import RocksDBStore


def save_truck_state(
    store: RocksDBStore,
    telemetry: Telemetry,
) -> None:
    """Save the latest telemetry state for a truck."""

    key = f"truck:{telemetry.truck_id}"

    store.put(
        key,
        {
            "truck_id": telemetry.truck_id,
            "temperature": telemetry.temperature,
            "timestamp": telemetry.timestamp.isoformat(),
        },
    )


def load_truck_state(
    store: RocksDBStore,
    truck_id: str,
) -> dict[str, object] | None:
    """Load the latest persisted state for a truck."""

    return store.get(f"truck:{truck_id}")


def get_truck_timestamp(
    store: RocksDBStore,
    truck_id: str,
) -> datetime | None:
    """Return the timestamp of the latest stored truck state."""

    state = load_truck_state(store, truck_id)

    if state is None:
        return None

    timestamp = state.get("timestamp")

    if not isinstance(timestamp, str):
        return None

    return datetime.fromisoformat(timestamp)