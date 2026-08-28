"""Prometheus metrics for StreamForge telemetry processing."""

from prometheus_client import Counter, Gauge


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

active_trucks = Gauge(
    "streamforge_active_trucks",
    "Current number of active trucks.",
)


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
    active_trucks.set(count)
