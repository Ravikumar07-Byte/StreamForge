import tempfile

import pytest

from backend.state.metrics_state import (
    DEFAULT_METRICS,
    increment_metric,
    load_metrics,
    save_metrics,
    set_metric,
)
from backend.state.rocksdb_store import RocksDBStore


def create_store():
    temp_dir = tempfile.TemporaryDirectory()
    store = RocksDBStore(temp_dir.name)
    return temp_dir, store


def test_load_metrics_returns_defaults_when_empty():
    temp_dir, store = create_store()

    try:
        metrics = load_metrics(store)

        assert metrics == DEFAULT_METRICS
    finally:
        store.close()
        temp_dir.cleanup()


def test_save_and_load_metrics():
    temp_dir, store = create_store()

    try:
        metrics = {
            "events_received": 100,
            "events_processed": 95,
            "events_invalid": 3,
            "events_late": 2,
            "active_trucks": 5,
        }

        save_metrics(store, metrics)

        loaded = load_metrics(store)

        assert loaded == metrics
    finally:
        store.close()
        temp_dir.cleanup()


def test_increment_metric():
    temp_dir, store = create_store()

    try:
        increment_metric(store, "events_received")
        increment_metric(store, "events_received", 4)

        metrics = load_metrics(store)

        assert metrics["events_received"] == 5
    finally:
        store.close()
        temp_dir.cleanup()


def test_set_metric():
    temp_dir, store = create_store()

    try:
        set_metric(store, "events_processed", 250)

        metrics = load_metrics(store)

        assert metrics["events_processed"] == 250
    finally:
        store.close()
        temp_dir.cleanup()


def test_increment_unknown_metric_raises_error():
    temp_dir, store = create_store()

    try:
        with pytest.raises(ValueError, match="Unknown metric"):
            increment_metric(store, "unknown_metric")
    finally:
        store.close()
        temp_dir.cleanup()


def test_set_unknown_metric_raises_error():
    temp_dir, store = create_store()

    try:
        with pytest.raises(ValueError, match="Unknown metric"):
            set_metric(store, "unknown_metric", 10)
    finally:
        store.close()
        temp_dir.cleanup()