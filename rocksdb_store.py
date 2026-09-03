"""
database/rocksdb_store.py
==========================
This is the "database creation" module for StreamForge.

Why RocksDB and not Postgres/Mongo/etc?
----------------------------------------
Each of the 20 parallel Python workers needs LOCAL, low-latency,
high-write-throughput key-value storage to hold rolling-window state
(sum, count, min, max per truck) without a network round trip per event.
RocksDB is an embedded LSM-tree store (no server process) — exactly what
Kafka Streams / Flink use internally for the same purpose on the JVM.

This module is 100% independent of Kafka/Faust. It can be dropped into
any backend (FastAPI, Flask, Django, a plain script) as shown at the
bottom of this file and in the project README.

Design:
- `RocksDBStateStore` creates/opens the on-disk DB (this is the literal
  "database creation" step) the first time it's instantiated.
- Every `put`/`delete` is first appended to a Kafka changelog topic
  (see `database/changelog.py`) BEFORE the local RocksDB write commits,
  giving the exactly-once recovery guarantee described in the use case.
- `recover_from_changelog()` rebuilds local state on a fresh/failed-over
  worker by replaying the compacted changelog topic.
"""
from __future__ import annotations

import json
import os
import threading
from typing import Iterator, Optional

from rocksdict import Rdict, Options

from database.changelog import ChangelogWriter, ChangelogReplayer


class RocksDBStateStore:
    """
    Embedded, crash-safe key-value store backing windowed aggregation state.

    Parameters
    ----------
    db_path:
        Filesystem path for this worker's RocksDB instance. Each worker
        node must use its OWN path (e.g. include the worker/partition id)
        since RocksDB is single-process-exclusive.
    changelog_topic:
        Kafka topic used as the write-ahead log for this store. Pass
        `None` to disable changelog-backed fault tolerance (e.g. in unit
        tests or for ephemeral/local-only use).
    bootstrap_servers:
        Kafka bootstrap servers, only needed if `changelog_topic` is set.
    """

    def __init__(
        self,
        db_path: str,
        changelog_topic: Optional[str] = None,
        bootstrap_servers: str = "localhost:9092",
        create_if_missing: bool = True,
    ) -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

        opts = Options()
        opts.create_if_missing(create_if_missing)
        # Tuning appropriate for many small, frequent writes (streaming
        # aggregation) rather than large batch loads.
        opts.set_max_write_buffer_number(4)
        opts.set_write_buffer_size(64 * 1024 * 1024)

        # --- literal "database creation" call ---
        self._db = Rdict(db_path, options=opts)

        self._lock = threading.RLock()
        self._changelog_enabled = changelog_topic is not None
        if self._changelog_enabled:
            self._changelog_writer = ChangelogWriter(
                topic=changelog_topic, bootstrap_servers=bootstrap_servers
            )
            self._changelog_replayer = ChangelogReplayer(
                topic=changelog_topic, bootstrap_servers=bootstrap_servers
            )
        else:
            self._changelog_writer = None
            self._changelog_replayer = None

    # ------------------------------------------------------------------ #
    # Core CRUD
    # ------------------------------------------------------------------ #
    def put(self, key: str, value: dict) -> None:
        """Write-ahead to Kafka changelog, then commit locally to RocksDB."""
        with self._lock:
            if self._changelog_enabled:
                self._changelog_writer.append(key=key, value=value, op="put")
            self._db[key] = json.dumps(value)

    def get(self, key: str) -> Optional[dict]:
        with self._lock:
            raw = self._db.get(key)
            return json.loads(raw) if raw is not None else None

    def delete(self, key: str) -> None:
        with self._lock:
            if self._changelog_enabled:
                self._changelog_writer.append(key=key, value={}, op="delete")
            if key in self._db:
                del self._db[key]

    def scan(self, prefix: str = "") -> Iterator[tuple[str, dict]]:
        """Range scan, e.g. store.scan(prefix='truck:') for every truck's state."""
        with self._lock:
            for key, raw in self._db.items():
                if key.startswith(prefix):
                    yield key, json.loads(raw)

    def close(self) -> None:
        with self._lock:
            self._db.close()
            if self._changelog_writer:
                self._changelog_writer.close()

    # ------------------------------------------------------------------ #
    # Fault tolerance
    # ------------------------------------------------------------------ #
    def recover_from_changelog(self) -> int:
        """
        Replay the compacted Kafka changelog topic into local RocksDB.
        Call this on worker startup, and especially after a partition is
        reassigned to this worker following another worker's crash.

        Returns the number of records replayed.
        """
        if not self._changelog_enabled:
            raise RuntimeError("Changelog is disabled for this store instance.")

        replayed = 0
        for record in self._changelog_replayer.replay():
            with self._lock:
                if record.op == "delete":
                    if record.key in self._db:
                        del self._db[record.key]
                else:
                    self._db[record.key] = json.dumps(record.value)
            replayed += 1
        return replayed

    def checkpoint(self, checkpoint_dir: str) -> None:
        """Optional: RocksDB native checkpoint for fast local snapshots,
        complementary to (not a replacement for) the Kafka changelog."""
        from rocksdict import Checkpoint

        os.makedirs(checkpoint_dir, exist_ok=True)
        Checkpoint(self._db).create_checkpoint(checkpoint_dir)

    # Context-manager convenience: `with RocksDBStateStore(...) as store:`
    def __enter__(self) -> "RocksDBStateStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


# ---------------------------------------------------------------------- #
# Example: dropping this into ANY backend, independent of Kafka/Faust
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    # Minimal smoke test / usage example you can run directly:
    #   python -m database.rocksdb_store
    store = RocksDBStateStore(db_path="./data/demo_state", changelog_topic=None)
    store.put("truck:1042", {"avg_temp": 71.4, "sample_count": 12})
    print("Read back:", store.get("truck:1042"))
    print("Scan 'truck:':", list(store.scan(prefix="truck:")))
    store.close()
