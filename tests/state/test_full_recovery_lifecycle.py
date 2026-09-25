import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

from backend.models.telemetry import Telemetry
from backend.state.changelog_recovery import restore_from_changelog
from backend.state.manager import PersistentStateManager
from backend.state.rocksdb_store import RocksDBStore
from backend.state.truck_state import save_truck_state


def test_full_worker_state_recovery_lifecycle():
    db_path = (
        f"data/test_full_recovery_lifecycle_"
        f"{uuid.uuid4().hex[:8]}"
    )

    truck_a = (
        f"TRUCK-A-{uuid.uuid4().hex[:8]}"
    )
    truck_b = (
        f"TRUCK-B-{uuid.uuid4().hex[:8]}"
    )
    truck_c = (
        f"TRUCK-C-{uuid.uuid4().hex[:8]}"
    )

    key_a = f"truck:{truck_a}"
    key_b = f"truck:{truck_b}"
    key_c = f"truck:{truck_c}"

    state_a_initial = {
        "truck_id": truck_a,
        "temperature": 25.0,
        "timestamp": "2026-09-25T12:00:00+00:00",
        "last_seen_at": "2026-09-25T12:00:00+00:00",
    }

    state_b_initial = {
        "truck_id": truck_b,
        "temperature": 30.0,
        "timestamp": "2026-09-25T12:00:00+00:00",
        "last_seen_at": "2026-09-25T12:00:00+00:00",
    }

    state_c_initial = {
        "truck_id": truck_c,
        "temperature": 28.0,
        "timestamp": "2026-09-25T12:00:00+00:00",
        "last_seen_at": "2026-09-25T12:00:00+00:00",
    }

    try:
        # ---------------------------------------------------------
        # 1. Previous worker persists initial states.
        # ---------------------------------------------------------
        with PersistentStateManager(
            db_path=db_path,
        ) as manager:
            manager.put(
                key=key_a,
                value=state_a_initial,
            )

            manager.put(
                key=key_b,
                value=state_b_initial,
            )

            manager.put(
                key=key_c,
                value=state_c_initial,
            )

            manager.flush()

        # ---------------------------------------------------------
        # 2. Publish an updated state for Truck A and Truck B.
        #
        # The changelog should contain multiple versions of
        # the same keys. Recovery must restore the latest state.
        # ---------------------------------------------------------
        producer = Producer(
            {
                "bootstrap.servers": (
                    "localhost:9092"
                ),
            }
        )

        state_a_updated = {
            "truck_id": truck_a,
            "temperature": 32.5,
            "timestamp": "2026-09-25T12:05:00+00:00",
            "last_seen_at": "2026-09-25T12:05:00+00:00",
        }

        state_b_updated = {
            "truck_id": truck_b,
            "temperature": 35.5,
            "timestamp": "2026-09-25T12:05:00+00:00",
            "last_seen_at": "2026-09-25T12:05:00+00:00",
        }

        producer.produce(
            topic="streamforge-state-changelog",
            key=key_a,
            value=json.dumps(
                state_a_updated,
                separators=(",", ":"),
            ),
        )

        producer.produce(
            topic="streamforge-state-changelog",
            key=key_b,
            value=json.dumps(
                state_b_updated,
                separators=(",", ":"),
            ),
        )

        producer.flush()

        # ---------------------------------------------------------
        # 3. Delete Truck C using a Kafka tombstone.
        # ---------------------------------------------------------
        producer.produce(
            topic="streamforge-state-changelog",
            key=key_c,
            value=None,
        )

        producer.flush()

        # ---------------------------------------------------------
        # 4. Simulate worker/local RocksDB failure.
        # ---------------------------------------------------------
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)

        # ---------------------------------------------------------
        # 5. New worker starts and restores state from Kafka.
        # ---------------------------------------------------------
        with RocksDBStore(db_path) as store:
            restored_count = restore_from_changelog(
                store,
            )

            assert restored_count >= 2

            # -----------------------------------------------------
            # 6. Latest Truck A state must be restored.
            # -----------------------------------------------------
            recovered_a = store.get(key_a)

            assert recovered_a is not None
            assert recovered_a["truck_id"] == truck_a
            assert recovered_a["temperature"] == 32.5
            assert recovered_a["timestamp"] == (
                "2026-09-25T12:05:00+00:00"
            )

            # -----------------------------------------------------
            # 7. Latest Truck B state must be restored.
            # -----------------------------------------------------
            recovered_b = store.get(key_b)

            assert recovered_b is not None
            assert recovered_b["truck_id"] == truck_b
            assert recovered_b["temperature"] == 35.5
            assert recovered_b["timestamp"] == (
                "2026-09-25T12:05:00+00:00"
            )

            # -----------------------------------------------------
            # 8. Truck C must remain deleted after recovery.
            # -----------------------------------------------------
            recovered_c = store.get(key_c)

            assert recovered_c is None

            # -----------------------------------------------------
            # 9. Worker continues processing new telemetry
            #    after recovery.
            # -----------------------------------------------------
            new_telemetry = Telemetry(
                truck_id=truck_a,
                temperature=40.0,
                timestamp=datetime(
                    2026,
                    9,
                    25,
                    12,
                    10,
                    tzinfo=timezone.utc,
                ),
            )

            save_truck_state(
                store,
                new_telemetry,
            )

            # -----------------------------------------------------
            # 10. Verify Truck A was updated normally.
            # -----------------------------------------------------
            updated_a = store.get(key_a)

            assert updated_a is not None
            assert updated_a["truck_id"] == truck_a
            assert updated_a["temperature"] == 40.0
            assert updated_a["timestamp"] == (
                "2026-09-25T12:10:00+00:00"
            )

            # -----------------------------------------------------
            # 11. Verify Truck B remains unchanged.
            # -----------------------------------------------------
            unchanged_b = store.get(key_b)

            assert unchanged_b is not None
            assert unchanged_b["truck_id"] == truck_b
            assert unchanged_b["temperature"] == 35.5

            # -----------------------------------------------------
            # 12. Verify Truck C is still absent.
            # -----------------------------------------------------
            assert store.get(key_c) is None

    finally:
        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)