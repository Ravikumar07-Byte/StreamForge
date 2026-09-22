import shutil
import uuid
from pathlib import Path

from backend.state.changelog_recovery import (
    restore_from_changelog,
)
from backend.state.manager import PersistentStateManager
from backend.state.rocksdb_store import RocksDBStore


def test_state_can_be_restored_from_kafka_changelog():
    db_path = (
        f"data/test_changelog_recovery_"
        f"{uuid.uuid4().hex[:8]}"
    )

    key = (
        f"recovery:test-"
        f"{uuid.uuid4().hex[:8]}"
    )

    state = {
        "truck_id": "TRUCK-RECOVERY-001",
        "temperature": 31.5,
        "speed": 58.0,
        "status": "active",
    }

    restored_count = 0

    try:
        # ---------------------------------------------------------
        # 1. Create original state and publish changelog.
        # ---------------------------------------------------------
        with PersistentStateManager(
            db_path=db_path,
        ) as manager:
            manager.put(
                key=key,
                value=state,
            )

            manager.flush()

            assert manager.get(key) == state

        # ---------------------------------------------------------
        # 2. Delete the local RocksDB database.
        #
        # This simulates a worker losing its local state.
        # ---------------------------------------------------------
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)

        # ---------------------------------------------------------
        # 3. Create a fresh RocksDB instance.
        # ---------------------------------------------------------
        with RocksDBStore(db_path) as store:
            assert not store.exists(key)

            # -----------------------------------------------------
            # 4. Replay Kafka changelog into fresh RocksDB.
            # -----------------------------------------------------
            restored_count = restore_from_changelog(
                store,
            )

            # -----------------------------------------------------
            # 5. Verify state was recovered.
            # -----------------------------------------------------
            assert restored_count >= 1

            assert store.exists(key)

            restored_state = store.get(key)

            assert restored_state == state

    finally:
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)