"""Kafka topic definitions for StreamForge."""

from backend.kafka.config import KAFKA_TELEMETRY_TOPIC


TRUCK_TELEMETRY_TOPIC = KAFKA_TELEMETRY_TOPIC

STATE_CHANGELOG_TOPIC = "streamforge-state-changelog"
