"""Tests for StreamForge persistent state and metrics."""

from pathlib import Path

from backend.metrics.prometheus import (
    active_trucks,
    record_invalid,
    record_late,
    record_processed,
    record_received,
    set_active_trucks,
    telemetry_events_invalid,
    telemetry_events_late,
    telemetry_events_processed,
    telemetry_events_received,
)
from backend.models.telemetry import Telemetry
from backend.state.recovery import (
    clear_recovery_state,
    load_recovery_state,
    save_recovery_state,
)
from backend.state.rocksdb_store import RocksDBStore
from backend.state.truck_state import (
    get_truck_last_seen,
    get_truck_timestamp,
    load_truck_state,
    save_truck_state,
)


def test_rocksdb_put_and_get(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    store.put(
        "truck:TRUCK-001",
        {"temperature": 72.5},
    )

    assert store.get("truck:TRUCK-001") == {
        "temperature": 72.5,
    }

    store.close()


def test_rocksdb_exists_and_delete(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    store.put(
        "truck:TRUCK-001",
        {"status": "active"},
    )

    assert store.exists("truck:TRUCK-001") is True

    store.delete("truck:TRUCK-001")

    assert store.exists("truck:TRUCK-001") is False
    assert store.get("truck:TRUCK-001") is None

    store.close()


def test_recovery_state_save_and_load(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    save_recovery_state(
        store,
        partition=2,
        offset=145,
    )

    state = load_recovery_state(store)

    assert state is not None
    assert state["partition"] == 2
    assert state["offset"] == 145
    assert "updated_at" in state

    store.close()


def test_recovery_state_clear(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    save_recovery_state(
        store,
        partition=1,
        offset=50,
    )

    assert load_recovery_state(store) is not None

    clear_recovery_state(store)

    assert load_recovery_state(store) is None

    store.close()


def test_metrics_record_events():
    before_received = telemetry_events_received._value.get()
    before_processed = telemetry_events_processed._value.get()
    before_invalid = telemetry_events_invalid._value.get()
    before_late = telemetry_events_late._value.get()

    record_received()
    record_processed()
    record_invalid()
    record_late()

    assert (
        telemetry_events_received._value.get()
        == before_received + 1
    )

    assert (
        telemetry_events_processed._value.get()
        == before_processed + 1
    )

    assert (
        telemetry_events_invalid._value.get()
        == before_invalid + 1
    )

    assert (
        telemetry_events_late._value.get()
        == before_late + 1
    )


def test_active_trucks_metric():
    set_active_trucks(7)

    assert active_trucks._value.get() == 7

    set_active_trucks(3)

    assert active_trucks._value.get() == 3


def test_save_and_load_truck_state(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    telemetry = Telemetry(
        truck_id="TRUCK-001",
        temperature=32.5,
    )

    save_truck_state(
        store,
        telemetry,
    )

    state = load_truck_state(
        store,
        "TRUCK-001",
    )

    assert state is not None
    assert state["truck_id"] == "TRUCK-001"
    assert state["temperature"] == 32.5
    assert state["timestamp"] == telemetry.timestamp.isoformat()
    assert "last_seen_at" in state

    store.close()


def test_get_truck_timestamp(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    telemetry = Telemetry(
        truck_id="TRUCK-002",
        temperature=29.5,
    )

    save_truck_state(
        store,
        telemetry,
    )

    timestamp = get_truck_timestamp(
        store,
        "TRUCK-002",
    )

    assert timestamp == telemetry.timestamp

    store.close()


def test_get_truck_last_seen(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    telemetry = Telemetry(
        truck_id="TRUCK-003",
        temperature=31.5,
    )

    save_truck_state(
        store,
        telemetry,
    )

    last_seen_at = get_truck_last_seen(
        store,
        "TRUCK-003",
    )

    assert last_seen_at is not None
    assert last_seen_at.tzinfo is not None

    store.close()


def test_missing_truck_state_returns_none(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    assert (
        load_truck_state(
            store,
            "TRUCK-999",
        )
        is None
    )

    assert (
        get_truck_timestamp(
            store,
            "TRUCK-999",
        )
        is None
    )

    assert (
        get_truck_last_seen(
            store,
            "TRUCK-999",
        )
        is None
    )

    store.close()