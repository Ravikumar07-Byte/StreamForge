"""Persistent dashboard metrics for StreamForge."""

from backend.state.rocksdb_store import RocksDBStore


METRICS_KEY = "streamforge:metrics"


DEFAULT_METRICS = {
    "events_received": 0,
    "events_processed": 0,
    "events_invalid": 0,
    "events_late": 0,
    "active_trucks": 0,
}


def load_metrics(store: RocksDBStore) -> dict[str, int]:
    """Load persistent dashboard metrics."""

    value = store.get(METRICS_KEY)

    if not isinstance(value, dict):
        return DEFAULT_METRICS.copy()

    metrics = DEFAULT_METRICS.copy()

    for key in metrics:
        stored_value = value.get(key)

        if isinstance(stored_value, (int, float)):
            metrics[key] = int(stored_value)

    return metrics


def save_metrics(
    store: RocksDBStore,
    metrics: dict[str, int],
) -> None:
    """Save dashboard metrics."""

    store.put(
        METRICS_KEY,
        {
            key: int(value)
            for key, value in metrics.items()
            if key in DEFAULT_METRICS
        },
    )


def increment_metric(
    store: RocksDBStore,
    metric: str,
    amount: int = 1,
) -> None:
    """Increment a persistent dashboard metric."""

    if metric not in DEFAULT_METRICS:
        raise ValueError(f"Unknown metric: {metric}")

    metrics = load_metrics(store)
    metrics[metric] += amount
    save_metrics(store, metrics)


def set_metric(
    store: RocksDBStore,
    metric: str,
    value: int,
) -> None:
    """Set a persistent dashboard metric."""

    if metric not in DEFAULT_METRICS:
        raise ValueError(f"Unknown metric: {metric}")

    metrics = load_metrics(store)
    metrics[metric] = int(value)
    save_metrics(store, metrics)