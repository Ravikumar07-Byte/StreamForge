"""Persistent latest-state storage for truck telemetry."""

from datetime import datetime, timedelta, timezone

from backend.models.telemetry import Telemetry
from backend.state.rocksdb_store import RocksDBStore


# A truck is considered active when it was seen within this window.
ACTIVE_TRUCK_WINDOW = timedelta(minutes=5)


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
            "last_seen_at": datetime.now(timezone.utc).isoformat(),
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


def get_truck_last_seen(
    store: RocksDBStore,
    truck_id: str,
) -> datetime | None:
    """Return when the truck was last seen by the consumer."""

    state = load_truck_state(store, truck_id)

    if state is None:
        return None

    last_seen_at = state.get("last_seen_at")

    if not isinstance(last_seen_at, str):
        return None

    return datetime.fromisoformat(last_seen_at)


def get_active_trucks(
    store: RocksDBStore,
    now: datetime | None = None,
) -> set[str]:
    """Return trucks seen within the active-truck window."""

    if now is None:
        now = datetime.now(timezone.utc)

    active_truck_ids: set[str] = set()

    for key in store.keys():
        if not key.startswith("truck:"):
            continue

        truck_id = key[len("truck:"):]

        if not truck_id:
            continue

        last_seen_at = get_truck_last_seen(
            store,
            truck_id,
        )

        if last_seen_at is None:
            continue

        # Ensure timezone-aware comparison.
        if last_seen_at.tzinfo is None:
            last_seen_at = last_seen_at.replace(
                tzinfo=timezone.utc
            )

        if now - last_seen_at <= ACTIVE_TRUCK_WINDOW:
            active_truck_ids.add(truck_id)

    return active_truck_ids