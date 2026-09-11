import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

INPUT_TOPIC = os.getenv(
    "KAFKA_INPUT_TOPIC",
    "streamforge-week2-benchmark",
)

OUTPUT_TOPIC = os.getenv(
    "KAFKA_PROCESSED_TOPIC",
    "streamforge-week2-benchmark-processed",
)

NUM_EVENTS = 100_000
NUM_PARTITIONS = 4


def verify_partitions():
    admin = AdminClient(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
        }
    )

    metadata = admin.list_topics(
        topic=INPUT_TOPIC,
        timeout=10,
    )

    topic = metadata.topics.get(INPUT_TOPIC)

    if topic is None:
        raise RuntimeError(f"Topic not found: {INPUT_TOPIC}")

    partition_count = len(topic.partitions)

    print(f"Input topic: {INPUT_TOPIC}")
    print(f"Partitions: {partition_count}")

    if partition_count != NUM_PARTITIONS:
        raise RuntimeError(
            f"Expected {NUM_PARTITIONS} partitions, "
            f"found {partition_count}"
        )

    print("PASS: Kafka input topic has 4 partitions")


def get_output_offsets():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "streamforge-week2-benchmark-counter",
            "enable.auto.commit": False,
        }
    )

    offsets = []

    metadata = consumer.list_topics(
        topic=OUTPUT_TOPIC,
        timeout=10,
    )

    partitions = metadata.topics[OUTPUT_TOPIC].partitions

    for partition in sorted(partitions):
        low, high = consumer.get_watermark_offsets(
            TopicPartition(
                OUTPUT_TOPIC,
                partition,
            ),
            timeout=10,
        )

        offsets.append(
            TopicPartition(
                OUTPUT_TOPIC,
                partition,
                high,
            )
        )

    consumer.close()

    return offsets


def create_event(index):
    truck_id = f"BENCH-TRUCK-{index:06d}"

    event = {
        "truck_id": truck_id,
        "temperature": 20.0 + (index % 100) * 0.1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return truck_id, json.dumps(event).encode("utf-8")


def produce_events():
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "1",
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 200000,
        }
    )

    partition_counts = [0] * NUM_PARTITIONS

    start = time.perf_counter()

    for index in range(NUM_EVENTS):
        partition = index % NUM_PARTITIONS

        key, value = create_event(index)

        while True:
            try:
                producer.produce(
                    topic=INPUT_TOPIC,
                    key=key,
                    value=value,
                    partition=partition,
                )
                break

            except BufferError:
                producer.poll(0.1)

        partition_counts[partition] += 1

        if index % 10000 == 0:
            producer.poll(0)

    producer.flush()

    elapsed = time.perf_counter() - start

    rate = NUM_EVENTS / elapsed

    print()
    print("========== PRODUCER RESULT ==========")
    print(f"Events produced : {NUM_EVENTS:,}")
    print(f"Time            : {elapsed:.3f} seconds")
    print(f"Producer rate   : {rate:,.2f} events/sec")

    print()
    print("Input partition distribution:")

    for partition, count in enumerate(partition_counts):
        print(
            f"  Partition {partition}: "
            f"{count:,} events"
        )

    return start


def wait_for_processed(offsets, timeout_seconds=120):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "streamforge-week2-result-counter",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )

    consumer.assign(offsets)

    processed = 0
    start = time.perf_counter()

    while processed < NUM_EVENTS:

        if time.perf_counter() - start > timeout_seconds:
            consumer.close()

            raise TimeoutError(
                f"Only {processed:,} of "
                f"{NUM_EVENTS:,} events processed "
                f"within {timeout_seconds} seconds."
            )

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            continue

        processed += 1

        if processed % 10000 == 0:
            print(
                f"Processed: "
                f"{processed:,}/{NUM_EVENTS:,}"
            )

    elapsed = time.perf_counter() - start

    consumer.close()

    return elapsed


def main():
    print("=" * 60)
    print("STREAMFORGE WEEK 2 - 100K THROUGHPUT AUDIT")
    print("=" * 60)

    print()

    verify_partitions()

    print()
    print("Reading current output offsets...")

    output_offsets = get_output_offsets()

    print("Starting 100,000 event benchmark...")
    print()

    producer_start = time.perf_counter()

    produce_events()

    print()
    print("Waiting for Bytewax to process all events...")

    processing_start = time.perf_counter()

    processing_elapsed = wait_for_processed(
        output_offsets
    )

    total_elapsed = time.perf_counter() - producer_start

    processing_rate = NUM_EVENTS / processing_elapsed
    end_to_end_rate = NUM_EVENTS / total_elapsed

    print()
    print("=" * 60)
    print("WEEK 2 THROUGHPUT AUDIT RESULT")
    print("=" * 60)

    print(f"Total events       : {NUM_EVENTS:,}")
    print(f"Processing time    : {processing_elapsed:.3f} seconds")
    print(
        f"Processing rate    : "
        f"{processing_rate:,.2f} events/sec"
    )

    print(
        f"End-to-end time    : "
        f"{total_elapsed:.3f} seconds"
    )

    print(
        f"End-to-end rate    : "
        f"{end_to_end_rate:,.2f} events/sec"
    )

    print()

    if processing_rate >= 100_000:
        print("PASS: 100,000 events/sec target achieved")
    else:
        print("RESULT: 100,000 events/sec target not achieved")

    print("=" * 60)


if __name__ == "__main__":
    main()
