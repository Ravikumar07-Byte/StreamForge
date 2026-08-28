"""Persistent state storage using RocksDB."""

from pathlib import Path
from typing import Optional

from rocksdict import Rdict


class RocksDBStore:
    """Simple key-value state store backed by RocksDB."""

    def __init__(self, path: str = "data/state"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = Rdict(str(self.path))

    def put(self, key: str, value: object) -> None:
        """Store or replace a value."""
        self.db[key] = value

    def get(self, key: str) -> Optional[object]:
        """Return a stored value or None."""
        return self.db.get(key)

    def delete(self, key: str) -> None:
        """Delete a stored key if it exists."""
        if key in self.db:
            del self.db[key]

    def exists(self, key: str) -> bool:
        """Return True when a key exists."""
        return key in self.db

    def close(self) -> None:
        """Close the RocksDB store."""
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
