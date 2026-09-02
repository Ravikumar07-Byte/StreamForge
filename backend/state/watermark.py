"""Persistent event-time watermark state for StreamForge."""

from datetime import datetime

from backend.state.rocksdb_store import RocksDBStore


WATERMARK_KEY = "streamforge:watermark"


def load_watermark(
    store: RocksDBStore,
) -> datetime | None:
    """Load the latest event-time watermark."""
    value = store.get(WATERMARK_KEY)

    if not isinstance(value, dict):
        return None

    timestamp = value.get("timestamp")

    if not isinstance(timestamp, str):
        return None

    return datetime.fromisoformat(timestamp)


def update_watermark(
    store: RocksDBStore,
    event_timestamp: datetime,
) -> datetime:
    """Advance the watermark without allowing it to move backward."""
    current_watermark = load_watermark(store)

    if current_watermark is None or event_timestamp > current_watermark:
        current_watermark = event_timestamp

        store.put(
            WATERMARK_KEY,
            {
                "timestamp": current_watermark.isoformat(),
            },
        )

    return current_watermark
