"""Tests for StreamForge persistent state and metrics."""

from datetime import timedelta
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
from backend.state.late_events import save_late_event, list_late_events
from backend.state.truck_state import (
    get_truck_last_seen,
    get_truck_timestamp,
    load_truck_state,
    save_truck_state,
)
from backend.state.window_state import (
    load_window_state,
    save_window_event,
)
from backend.state.watermark import (
    load_watermark,
    update_watermark,
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


def test_save_and_load_window_state(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    window_start = Telemetry(
        truck_id="TRUCK-001",
        temperature=30.0,
    ).timestamp.replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    first = save_window_event(
        store,
        "TRUCK-001",
        window_start,
        30.0,
    )

    second = save_window_event(
        store,
        "TRUCK-001",
        window_start,
        40.0,
    )

    state = load_window_state(
        store,
        "TRUCK-001",
        window_start,
    )

    assert first["event_count"] == 1
    assert second["event_count"] == 2

    assert state is not None
    assert state["truck_id"] == "TRUCK-001"
    assert state["event_count"] == 2
    assert state["temperature_sum"] == 70.0
    assert state["temperature_average"] == 35.0

    store.close()


def test_window_state_keeps_trucks_separate(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    window_start = Telemetry(
        truck_id="TRUCK-001",
        temperature=30.0,
    ).timestamp.replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    save_window_event(
        store,
        "TRUCK-001",
        window_start,
        30.0,
    )

    save_window_event(
        store,
        "TRUCK-002",
        window_start,
        50.0,
    )

    truck_1 = load_window_state(
        store,
        "TRUCK-001",
        window_start,
    )

    truck_2 = load_window_state(
        store,
        "TRUCK-002",
        window_start,
    )

    assert truck_1 is not None
    assert truck_2 is not None

    assert truck_1["event_count"] == 1
    assert truck_1["temperature_average"] == 30.0

    assert truck_2["event_count"] == 1
    assert truck_2["temperature_average"] == 50.0

    store.close()



def test_watermark_advances_and_persists(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    first_timestamp = Telemetry(
        truck_id="TRUCK-001",
        temperature=30.0,
    ).timestamp

    second_timestamp = first_timestamp.replace(
        second=first_timestamp.second + 1,
    )

    first_watermark = update_watermark(
        store,
        first_timestamp,
    )

    second_watermark = update_watermark(
        store,
        second_timestamp,
    )

    loaded_watermark = load_watermark(store)

    assert first_watermark == first_timestamp
    assert second_watermark == second_timestamp
    assert loaded_watermark == second_timestamp

    store.close()


def test_watermark_never_moves_backward(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    timestamp = Telemetry(
        truck_id="TRUCK-001",
        temperature=30.0,
    ).timestamp

    newer_timestamp = timestamp.replace(
        second=timestamp.second + 10,
    )

    older_timestamp = timestamp

    update_watermark(
        store,
        newer_timestamp,
    )

    watermark = update_watermark(
        store,
        older_timestamp,
    )

    assert watermark == newer_timestamp
    assert load_watermark(store) == newer_timestamp

    store.close()


def test_save_and_list_late_event(tmp_path: Path):
    store = RocksDBStore(str(tmp_path / "state"))

    telemetry = Telemetry(
        truck_id="TRUCK-001",
        temperature=45.5,
    )

    from datetime import timedelta

    watermark = telemetry.timestamp + timedelta(seconds=120)

    state = save_late_event(
        store,
        telemetry,
        watermark,
    )

    events = list_late_events(store)

    assert state["truck_id"] == "TRUCK-001"
    assert state["temperature"] == 45.5
    assert state["timestamp"] == telemetry.timestamp.isoformat()
    assert state["watermark"] == watermark.isoformat()

    assert len(events) == 1
    assert events[0]["truck_id"] == "TRUCK-001"

    store.close()




def test_on_time_event_updates_five_minute_window(tmp_path: Path):
    from datetime import datetime, timezone

    from backend.state.window_state import load_window_state, save_window_event
    from backend.state.watermark import load_watermark, update_watermark
    from backend.streaming.windowing import get_five_minute_window_start

    store = RocksDBStore(str(tmp_path / "state"))

    timestamp = datetime(2026, 9, 2, 12, 7, 30, tzinfo=timezone.utc)
    window_start = get_five_minute_window_start(timestamp)

    watermark = update_watermark(store, timestamp)

    state = save_window_event(
        store,
        "TRUCK-001",
        window_start,
        45.5,
    )

    assert watermark == timestamp
    assert load_watermark(store) == timestamp
    assert state["truck_id"] == "TRUCK-001"
    assert state["event_count"] == 1
    assert state["temperature_average"] == 45.5

    loaded = load_window_state(
        store,
        "TRUCK-001",
        window_start,
    )

    assert loaded == state


def test_late_event_is_persisted_without_updating_window(tmp_path: Path):
    from datetime import datetime, timedelta, timezone

    from backend.state.late_events import list_late_events, save_late_event
    from backend.state.window_state import load_window_state
    from backend.state.watermark import update_watermark
    from backend.streaming.windowing import (
        get_five_minute_window_start,
        is_late_event,
    )

    store = RocksDBStore(str(tmp_path / "state"))

    watermark = datetime(
        2026,
        9,
        2,
        12,
        10,
        0,
        tzinfo=timezone.utc,
    )

    late_timestamp = watermark - timedelta(seconds=120)

    assert is_late_event(
        late_timestamp,
        watermark,
        allowed_lateness_seconds=60,
    )

    window_start = get_five_minute_window_start(late_timestamp)

    state = save_late_event(
        store,
        Telemetry(
            truck_id="TRUCK-001",
            temperature=49.0,
            timestamp=late_timestamp,
        ),
        watermark,
    )

    assert state["truck_id"] == "TRUCK-001"
    assert state["temperature"] == 49.0

    late_events = list_late_events(store)

    assert len(late_events) == 1
    assert late_events[0]["truck_id"] == "TRUCK-001"

    assert load_window_state(
        store,
        "TRUCK-001",
        window_start,
    ) is None
