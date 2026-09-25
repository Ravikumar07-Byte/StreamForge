import shutil
import uuid
from pathlib import Path

from backend.consumers.telemetry_consumer import (
    restore_state_if_needed,
)
from backend.state.changelog_recovery import (
    restore_from_changelog,
)
from backend.state.manager import PersistentStateManager
from backend.state.rocksdb_store import RocksDBStore


def test_worker_state_can_be_recovered_after_local_state_loss():
    db_path = (
        f"data/test_worker_recovery_"
        f"{uuid.uuid4().hex[:8]}"
    )

    key = (
        f"truck:TRUCK-WORKER-"
        f"{uuid.uuid4().hex[:8]}"
    )

    state = {
        "truck_id": "TRUCK-WORKER-001",
        "temperature": 32.5,
        "speed": 54.0,
        "status": "active",
    }

    try:
        # ---------------------------------------------------------
        # 1. Simulate the original worker.
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
        # 2. Simulate worker/local RocksDB failure.
        # ---------------------------------------------------------
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)

        # ---------------------------------------------------------
        # 3. Simulate a new worker starting.
        # ---------------------------------------------------------
        with RocksDBStore(db_path) as store:
            assert not store.exists(key)

            # -----------------------------------------------------
            # 4. New worker restores state from Kafka.
            # -----------------------------------------------------
            restored_count = restore_from_changelog(
                store,
            )

            # -----------------------------------------------------
            # 5. Verify state recovery.
            # -----------------------------------------------------
            assert restored_count >= 1
            assert store.exists(key)

            recovered_state = store.get(key)

            assert recovered_state == state

    finally:
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)


def test_worker_startup_integration_recovers_missing_local_state():
    db_path = (
        f"data/test_worker_startup_"
        f"{uuid.uuid4().hex[:8]}"
    )

    key = (
        f"truck:TRUCK-STARTUP-"
        f"{uuid.uuid4().hex[:8]}"
    )

    state = {
        "truck_id": "TRUCK-STARTUP-001",
        "temperature": 29.5,
        "timestamp": "2026-09-25T14:00:00+00:00",
        "last_seen_at": "2026-09-25T14:00:00+00:00",
    }

    try:
        # ---------------------------------------------------------
        # 1. Create state exactly as a previous worker would.
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
        # 2. Simulate local RocksDB loss.
        # ---------------------------------------------------------
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)

        # ---------------------------------------------------------
        # 3. Simulate worker startup.
        #
        # restore_state_if_needed() is the actual startup
        # integration function used by telemetry_consumer.run().
        # ---------------------------------------------------------
        with RocksDBStore(db_path) as store:
            assert not store.exists(key)

            recovered = restore_state_if_needed(store)

            # -----------------------------------------------------
            # 4. Startup must report successful recovery.
            # -----------------------------------------------------
            assert recovered is True

            # -----------------------------------------------------
            # 5. Verify the worker's local state was restored.
            # -----------------------------------------------------
            assert store.exists(key)

            recovered_state = store.get(key)

            assert recovered_state == state

    finally:
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)