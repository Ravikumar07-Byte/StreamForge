import shutil
import uuid
from pathlib import Path

from backend.state.changelog_recovery import restore_from_changelog
from backend.state.manager import PersistentStateManager
from backend.state.rocksdb_store import RocksDBStore


def test_multiple_truck_states_are_recovered_after_worker_failure():
    db_path = (
        f"data/test_multiple_state_recovery_"
        f"{uuid.uuid4().hex[:8]}"
    )

    states = {
        f"truck:TRUCK-MULTI-001-{uuid.uuid4().hex[:6]}": {
            "truck_id": "TRUCK-MULTI-001",
            "temperature": 28.5,
            "speed": 45.0,
            "status": "active",
        },
        f"truck:TRUCK-MULTI-002-{uuid.uuid4().hex[:6]}": {
            "truck_id": "TRUCK-MULTI-002",
            "temperature": 31.0,
            "speed": 52.0,
            "status": "active",
        },
        f"truck:TRUCK-MULTI-003-{uuid.uuid4().hex[:6]}": {
            "truck_id": "TRUCK-MULTI-003",
            "temperature": 26.5,
            "speed": 48.0,
            "status": "active",
        },
    }

    try:
        # ---------------------------------------------------------
        # 1. Worker 1 persists multiple independent truck states.
        # ---------------------------------------------------------
        with PersistentStateManager(
            db_path=db_path,
        ) as manager:

            for key, state in states.items():
                manager.put(
                    key=key,
                    value=state,
                )

            manager.flush()

            # Verify all states exist locally.
            for key, state in states.items():
                assert manager.get(key) == state

        # ---------------------------------------------------------
        # 2. Simulate worker failure and local state loss.
        # ---------------------------------------------------------
        path = Path(db_path)

        assert path.exists()

        shutil.rmtree(path)

        # ---------------------------------------------------------
        # 3. Worker 2 starts with an empty local RocksDB.
        # ---------------------------------------------------------
        with RocksDBStore(db_path) as store:

            for key in states:
                assert not store.exists(key)

            # -----------------------------------------------------
            # 4. Replay Kafka changelog.
            # -----------------------------------------------------
            restored_count = restore_from_changelog(
                store,
            )

            # -----------------------------------------------------
            # 5. All states must be restored.
            # -----------------------------------------------------
            assert restored_count >= len(states)

            for key, expected_state in states.items():
                assert store.exists(key)

                recovered_state = store.get(key)

                assert recovered_state == expected_state

    finally:
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)