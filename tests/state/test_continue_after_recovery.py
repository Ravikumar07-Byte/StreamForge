import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from backend.models.telemetry import Telemetry
from backend.state.changelog_recovery import restore_from_changelog
from backend.state.manager import PersistentStateManager
from backend.state.rocksdb_store import RocksDBStore
from backend.state.truck_state import save_truck_state


def test_worker_continues_processing_after_state_recovery():
    db_path = (
        f"data/test_continue_after_recovery_"
        f"{uuid.uuid4().hex[:8]}"
    )

    truck_id = (
        f"TRUCK-CONTINUE-"
        f"{uuid.uuid4().hex[:8]}"
    )

    key = f"truck:{truck_id}"

    recovered_state = {
        "truck_id": truck_id,
        "temperature": 25.0,
        "timestamp": "2026-09-25T12:00:00+00:00",
        "last_seen_at": "2026-09-25T12:00:00+00:00",
    }

    try:
        # ---------------------------------------------------------
        # 1. Previous worker persists state to Kafka changelog.
        # ---------------------------------------------------------
        with PersistentStateManager(
            db_path=db_path,
        ) as manager:
            manager.put(
                key=key,
                value=recovered_state,
            )

            manager.flush()

        # ---------------------------------------------------------
        # 2. Simulate local RocksDB loss.
        # ---------------------------------------------------------
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)

        # ---------------------------------------------------------
        # 3. New worker starts and recovers state.
        # ---------------------------------------------------------
        with RocksDBStore(db_path) as store:
            restored_count = restore_from_changelog(
                store,
            )

            assert restored_count >= 1
            assert store.get(key) == recovered_state

            # -----------------------------------------------------
            # 4. Worker continues processing a new telemetry event.
            # -----------------------------------------------------
            telemetry = Telemetry(
                truck_id=truck_id,
                temperature=32.5,
                timestamp=datetime(
                    2026,
                    9,
                    25,
                    12,
                    5,
                    tzinfo=timezone.utc,
                ),
            )

            save_truck_state(
                store,
                telemetry,
            )

            # -----------------------------------------------------
            # 5. Verify the recovered state was updated normally.
            # -----------------------------------------------------
            updated_state = store.get(key)

            assert updated_state is not None
            assert updated_state["truck_id"] == truck_id
            assert updated_state["temperature"] == 32.5
            assert updated_state["timestamp"] == (
                "2026-09-25T12:05:00+00:00"
            )

    finally:
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)