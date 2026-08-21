from backend.kafka.consumer import TelemetryConsumer


def test_consumer_can_be_created():
    consumer = TelemetryConsumer(
        group_id="streamforge-test-consumer"
    )

    assert consumer is not None

    consumer.close()
