"""Kafka configuration for StreamForge."""

import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TELEMETRY_TOPIC = os.getenv(
    "KAFKA_TELEMETRY_TOPIC",
    "truck-telemetry",
)

KAFKA_PRODUCER_CLIENT_ID = os.getenv(
    "KAFKA_PRODUCER_CLIENT_ID",
    "streamforge-telemetry-producer",
)
