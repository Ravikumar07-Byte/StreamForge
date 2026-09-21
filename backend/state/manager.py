"""Persistent state manager with Kafka changelog support."""

from backend.kafka.state_changelog import StateChangelogProducer
from backend.state.rocksdb_store import RocksDBStore


class PersistentStateManager:
    """Manage persistent state in RocksDB and Kafka changelog."""

    def __init__(
        self,
        db_path: str = "data/state",
    ) -> None:
        self.store = RocksDBStore(db_path)
        self.changelog = StateChangelogProducer()

    def put(
        self,
        key: str,
        value: object,
    ) -> None:
        """Persist state locally and publish its changelog."""

        self.store.put(key, value)

        self.changelog.publish(
            key=key,
            state=value,
        )

    def get(
        self,
        key: str,
    ) -> object | None:
        """Return persisted state."""

        return self.store.get(key)

    def exists(
        self,
        key: str,
    ) -> bool:
        """Return whether state exists."""

        return self.store.exists(key)

    def delete(
        self,
        key: str,
    ) -> None:
        """Delete state from local storage."""

        self.store.delete(key)

    def flush(self) -> None:
        """Flush pending changelog messages."""

        self.changelog.flush()

    def close(self) -> None:
        """Close state resources."""

        self.store.close()

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):
        self.close()
