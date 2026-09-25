import shutil
import uuid
from pathlib import Path

from backend.state.changelog_recovery import restore_from_changelog
from backend.state.manager import PersistentStateManager
from backend.state.rocksdb_store import RocksDBStore


def test_worker_restart_recovers_previous_state():
    db_path = (
        f"data/test_worker_restart_"
        f"{uuid.uuid4().hex[:8]}"
    )

    key = (
        f"truck:TRUCK-RESTART-"
        f"{uuid.uuid4().hex[:8]}"
    )

    state = {
        "truck_id": "TRUCK-RESTART-001",
        "temperature": 30.5,
        "speed": 52.0,
        "status": "active",
    }

    try:
        # ---------------------------------------------------------
        # 1. Worker 1 starts and persists state.
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
        # 2. Worker 1 stops.
        #
        # Simulate loss of its local RocksDB state.
        # Kafka changelog remains available.
        # ---------------------------------------------------------
        path = Path(db_path)

        assert path.exists()

        shutil.rmtree(path)

        # ---------------------------------------------------------
        # 3. Worker 2 starts with a fresh local state store.
        # ---------------------------------------------------------
        with RocksDBStore(db_path) as store:
            assert not store.exists(key)

            # -----------------------------------------------------
            # 4. Worker 2 restores state from Kafka.
            # -----------------------------------------------------
            restored_count = restore_from_changelog(
                store,
            )

            # -----------------------------------------------------
            # 5. Verify previous worker state was recovered.
            # -----------------------------------------------------
            assert restored_count >= 1
            assert store.exists(key)

            recovered_state = store.get(key)

            assert recovered_state == state

    finally:
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)