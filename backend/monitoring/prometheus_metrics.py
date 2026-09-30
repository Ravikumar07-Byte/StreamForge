"""Prometheus metrics for the StreamForge streaming pipeline."""

from prometheus_client import Counter, Gauge, Histogram


# ============================================================
# EVENT METRICS
# ============================================================

events_received_total = Counter(
    "streamforge_events_received_total",
    "Total number of telemetry events received by the consumer",
)

events_processed_total = Counter(
    "streamforge_events_processed_total",
    "Total number of telemetry events successfully processed",
)

events_invalid_total = Counter(
    "streamforge_events_invalid_total",
    "Total number of invalid telemetry events",
)

events_late_total = Counter(
    "streamforge_events_late_total",
    "Total number of late telemetry events",
)


# ============================================================
# PROCESSING METRICS
# ============================================================

processing_latency_seconds = Histogram(
    "streamforge_processing_latency_seconds",
    "Telemetry event processing latency in seconds",
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
    "Current telemetry processing throughput in events per second",
)


# ============================================================
# STREAM STATE
# ============================================================

active_trucks = Gauge(
    "streamforge_active_trucks",
    "Number of currently active trucks",
)

worker_status = Gauge(
    "streamforge_worker_status",
    "StreamForge worker status: 1=running, 0=stopped",
)

last_event_timestamp_seconds = Gauge(
    "streamforge_last_event_timestamp_seconds",
    "Unix timestamp of the most recently processed telemetry event",
)


# ============================================================
# ERROR / ACTIVITY METRICS
# ============================================================

processing_errors_total = Counter(
    "streamforge_processing_errors_total",
    "Total number of stream processing errors",
)

processing_events_in_progress = Gauge(
    "streamforge_processing_events_in_progress",
    "Number of telemetry events currently being processed",
)