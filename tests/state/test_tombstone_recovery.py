import shutil
import uuid
from pathlib import Path

from backend.kafka.state_changelog import StateChangelogProducer
from backend.state.changelog_recovery import restore_from_changelog
from backend.state.rocksdb_store import RocksDBStore


def test_tombstone_removes_state_during_recovery():
    db_path = (
        f"data/test_tombstone_recovery_"
        f"{uuid.uuid4().hex[:8]}"
    )

    key = (
        f"truck:TRUCK-TOMBSTONE-"
        f"{uuid.uuid4().hex[:8]}"
    )

    state = {
        "truck_id": "TRUCK-TOMBSTONE-001",
        "temperature": 28.5,
        "timestamp": "2026-09-25T11:00:00+00:00",
        "last_seen_at": "2026-09-25T11:00:00+00:00",
    }

    producer = StateChangelogProducer()

    try:
        # ---------------------------------------------------------
        # 1. Publish the original state.
        # ---------------------------------------------------------
        producer.publish(
            key=key,
            state=state,
        )

        # ---------------------------------------------------------
        # 2. Publish tombstone.
        # ---------------------------------------------------------
        producer.producer.produce(
            topic="streamforge-state-changelog",
            key=key,
            value=None,
        )

        producer.flush()

        # ---------------------------------------------------------
        # 3. Create a new empty RocksDB instance.
        # ---------------------------------------------------------
        with RocksDBStore(db_path) as store:
            assert not store.exists(key)

            # -----------------------------------------------------
            # 4. Replay the changelog.
            # -----------------------------------------------------
            restored_count = restore_from_changelog(
                store,
            )

            # -----------------------------------------------------
            # 5. Tombstone must remove the state.
            # -----------------------------------------------------
            assert not store.exists(key)

            assert store.get(key) is None

            # The state record itself was encountered.
            assert restored_count >= 1

    finally:
        producer.flush()

        path = Path(db_path)

        if path.exists():
            shutil.rmtree(path)