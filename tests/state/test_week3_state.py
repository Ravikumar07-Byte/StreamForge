from datetime import datetime, timezone
from pathlib import Path
import shutil

from backend.models.telemetry import Telemetry
from backend.state.metrics_state import (
    increment_metric,
    load_metrics,
    save_metrics,
)
from backend.state.recovery import (
    load_recovery_state,
    save_recovery_state,
)
from backend.state.rocksdb_store import RocksDBStore
from backend.state.truck_state import (
    get_active_trucks,
    load_truck_state,
    save_truck_state,
)


TEST_DB = "data/test_week3_state"


def reset_test_db():
    """Reset the isolated test database before each test."""

    path = Path(TEST_DB)

    if path.exists():
        shutil.rmtree(path)


def test_rocksdb_basic_persistence():
    reset_test_db()

    with RocksDBStore(TEST_DB) as store:
        store.put(
            "test:key",
            {"value": "week3"},
        )

    with RocksDBStore(TEST_DB) as store:
        assert store.get("test:key") == {
            "value": "week3"
        }


def test_metrics_persistence():
    reset_test_db()

    with RocksDBStore(TEST_DB) as store:
        save_metrics(
            store,
            {
                "events_received": 10,
                "events_processed": 8,
                "events_invalid": 2,
                "events_late": 1,
                "active_trucks": 3,
            },
        )

    with RocksDBStore(TEST_DB) as store:
        metrics = load_metrics(store)

        assert metrics["events_received"] == 10
        assert metrics["events_processed"] == 8
        assert metrics["events_invalid"] == 2
        assert metrics["events_late"] == 1
        assert metrics["active_trucks"] == 3


def test_metric_increment_persists():
    reset_test_db()

    with RocksDBStore(TEST_DB) as store:
        increment_metric(
            store,
            "events_processed",
            5,
        )

    with RocksDBStore(TEST_DB) as store:
        metrics = load_metrics(store)

        assert metrics["events_processed"] == 5


def test_recovery_state_persists():
    reset_test_db()

    with RocksDBStore(TEST_DB) as store:
        save_recovery_state(
            store,
            partition=0,
            offset=12345,
        )

    with RocksDBStore(TEST_DB) as store:
        recovery = load_recovery_state(store)

        assert recovery is not None
        assert recovery["partition"] == 0
        assert recovery["offset"] == 12345


def test_truck_state_persists():
    reset_test_db()

    telemetry = Telemetry(
        truck_id="TRUCK-000001",
        temperature=27.5,
        timestamp=datetime.now(timezone.utc),
    )

    with RocksDBStore(TEST_DB) as store:
        save_truck_state(
            store,
            telemetry,
        )

    with RocksDBStore(TEST_DB) as store:
        state = load_truck_state(
            store,
            "TRUCK-000001",
        )

        assert state is not None
        assert state["truck_id"] == "TRUCK-000001"
        assert state["temperature"] == 27.5


def test_active_truck_detection():
    reset_test_db()

    telemetry = Telemetry(
        truck_id="TRUCK-000002",
        temperature=25.0,
        timestamp=datetime.now(timezone.utc),
    )

    now = datetime.now(timezone.utc)

    with RocksDBStore(TEST_DB) as store:
        save_truck_state(
            store,
            telemetry,
        )

        active = get_active_trucks(
            store,
            now=now,
        )

        assert "TRUCK-000002" in active
