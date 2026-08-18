"""Kafka administration utilities."""

from confluent_kafka.admin import AdminClient

from backend.kafka.config import KAFKA_BOOTSTRAP_SERVERS


def create_admin_client() -> AdminClient:
    """Create a Kafka AdminClient."""
    return AdminClient(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )


def kafka_is_available() -> bool:
    """Check whether the Kafka broker is reachable."""
    try:
        admin = create_admin_client()
        admin.list_topics(timeout=5)
        return True
    except Exception:
        return False
