"""Prometheus metrics for StreamForge telemetry processing."""

import time

from prometheus_client import Counter, Gauge, Histogram


# ============================================================
# EVENT METRICS
# ============================================================

telemetry_events_received = Counter(
    "streamforge_telemetry_events_received_total",
    "Total telemetry events received.",
)

telemetry_events_processed = Counter(
    "streamforge_telemetry_events_processed_total",
    "Total telemetry events successfully processed.",
)

telemetry_events_invalid = Counter(
    "streamforge_telemetry_events_invalid_total",
    "Total invalid telemetry events rejected.",
)

telemetry_events_late = Counter(
    "streamforge_telemetry_events_late_total",
    "Total telemetry events identified as late.",
)


# ============================================================
# STREAM STATE
# ============================================================

active_trucks = Gauge(
    "streamforge_active_trucks",
    "Current number of active trucks.",
    multiprocess_mode="max",
)

worker_status = Gauge(
    "streamforge_worker_status",
    "StreamForge worker status: 1=running, 0=stopped.",
    multiprocess_mode="max",
)

last_event_timestamp_seconds = Gauge(
    "streamforge_last_event_timestamp_seconds",
    "Unix timestamp of the most recently processed telemetry event.",
    multiprocess_mode="max",
)


# ============================================================
# PROCESSING METRICS
# ============================================================

processing_latency_seconds = Histogram(
    "streamforge_processing_latency_seconds",
    "Telemetry event processing latency in seconds.",
    buckets=(
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
    ),
)

processing_throughput_events_per_second = Gauge(
    "streamforge_processing_throughput_events_per_second",
    "Current telemetry processing throughput in events per second.",
    multiprocess_mode="max",
)

processing_errors_total = Counter(
    "streamforge_processing_errors_total",
    "Total number of stream processing errors.",
)

processing_events_in_progress = Gauge(
    "streamforge_processing_events_in_progress",
    "Number of telemetry events currently being processed.",
    multiprocess_mode="max",
)


# ============================================================
# EVENT RECORDING HELPERS
# ============================================================

def record_received() -> None:
    """Record a received telemetry event."""
    telemetry_events_received.inc()


def record_processed() -> None:
    """Record a successfully processed telemetry event."""
    telemetry_events_processed.inc()


def record_invalid() -> None:
    """Record an invalid telemetry event."""
    telemetry_events_invalid.inc()


def record_late() -> None:
    """Record a late telemetry event."""
    telemetry_events_late.inc()


def set_active_trucks(count: int) -> None:
    """Set the current number of active trucks."""
    active_trucks.set(max(0, count))


# ============================================================
# WEEK 4 MONITORING HELPERS
# ============================================================

def set_worker_status(running: bool) -> None:
    """Set worker health status."""
    worker_status.set(1 if running else 0)


def record_processing_error() -> None:
    """Record a stream processing error."""
    processing_errors_total.inc()


def set_last_event_timestamp(timestamp_seconds: float) -> None:
    """Set timestamp of the latest processed event."""
    last_event_timestamp_seconds.set(max(0.0, timestamp_seconds))


def observe_processing_latency(start_time: float) -> float:
    """
    Record processing latency.

    Args:
        start_time: Value returned by time.perf_counter()
            before processing began.

    Returns:
        Processing duration in seconds.
    """
    latency = time.perf_counter() - start_time

    processing_latency_seconds.observe(
        max(0.0, latency)
    )

    return latency


def set_processing_throughput(
    events_per_second: float,
) -> None:
    """Set current processing throughput."""
    processing_throughput_events_per_second.set(
        max(0.0, events_per_second)
    )


def set_events_in_progress(count: int) -> None:
    """Set the number of events currently being processed."""
    processing_events_in_progress.set(
        max(0, count)
    )