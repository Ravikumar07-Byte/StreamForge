"""Recovery state utilities."""

from datetime import datetime, timezone
from typing import Optional

from backend.state.rocksdb_store import RocksDBStore


RECOVERY_KEY = "streamforge:recovery"


def save_recovery_state(
    store: RocksDBStore,
    partition: int,
    offset: int,
) -> None:
    """Persist the latest Kafka processing position."""
    store.put(
        RECOVERY_KEY,
        {
            "partition": partition,
            "offset": offset,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def load_recovery_state(
    store: RocksDBStore,
) -> Optional[dict[str, object]]:
    """Load the latest persisted processing position."""
    value = store.get(RECOVERY_KEY)

    if value is None:
        return None

    return value


def clear_recovery_state(store: RocksDBStore) -> None:
    """Remove persisted recovery information."""
    store.delete(RECOVERY_KEY)
