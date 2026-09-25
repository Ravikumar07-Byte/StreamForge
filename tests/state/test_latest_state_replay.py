import shutil
import uuid
from pathlib import Path

from backend.state.changelog_recovery import restore_from_changelog
from backend.state.manager import PersistentStateManager
from backend.state.rocksdb_store import RocksDBStore


def test_latest_state_wins_during_recovery():
    db_path = (
        f"data/test_latest_state_"
        f"{uuid.uuid4().hex[:8]}"
    )

    key = (
        f"truck:TRUCK-LATEST-"
        f"{uuid.uuid4().hex[:8]}"
    )

    first_state = {
        "truck_id": "TRUCK-LATEST-001",
        "temperature": 25.0,
        "timestamp": "2026-09-25T10:00:00+00:00",
        "last_seen_at": "2026-09-25T10:00:00+00:00",
    }

    latest_state = {
        "truck_id": "TRUCK-LATEST-001",
        "temperature": 31.5,
        "timestamp": "2026-09-25T10:05:00+00:00",
        "last_seen_at": "2026-09-25T10:05:00+00:00",
    }

    try:
        # ---------------------------------------------------------
        # 1. Original worker writes the first state.
        # ---------------------------------------------------------
        with PersistentStateManager(
            db_path=db_path,
        ) as manager:
            manager.put(
                key=key,
                value=first_state,
            )

            manager.flush()

            # -----------------------------------------------------
            # 2. Same worker updates the same key.
            # -----------------------------------------------------
            manager.put(
                key=key,
                value=latest_state,
            )

            manager.flush()

            assert manager.get(key) == latest_state

        # ---------------------------------------------------------
        # 3. Simulate worker/local RocksDB failure.
        # ---------------------------------------------------------
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)

        # ---------------------------------------------------------
        # 4. New worker starts with empty local state.
        # ---------------------------------------------------------
        with RocksDBStore(db_path) as store:
            assert not store.exists(key)

            # -----------------------------------------------------
            # 5. Replay the Kafka changelog.
            # -----------------------------------------------------
            restored_count = restore_from_changelog(
                store,
            )

            assert restored_count >= 2

            # -----------------------------------------------------
            # 6. Latest changelog record must win.
            # -----------------------------------------------------
            assert store.exists(key)

            recovered_state = store.get(key)

            assert recovered_state == latest_state

            assert recovered_state != first_state

    finally:
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)