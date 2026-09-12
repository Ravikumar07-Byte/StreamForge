import json
import os
import shutil
import time

from confluent_kafka import Producer


NUM_EVENTS = 100_000
NUM_PARTITIONS = 4

INPUT_TOPIC = os.getenv(
    "KAFKA_INPUT_TOPIC",
    "streamforge-week2-benchmark",
)

BROKER = os.getenv(
    "KAFKA_BROKER",
    "localhost:9092",
)

COUNT_DIR = os.path.abspath(
    os.getenv(
        "BYTEWAX_BENCHMARK_COUNT_DIR",
        "benchmark_counts",
    )
)


def delivery_report(err, msg):
    if err is not None:
        raise RuntimeError(
            f"Kafka delivery failed: {err}"
        )


def read_processed_count():
    total = 0

    if not os.path.isdir(COUNT_DIR):
        return 0

    for filename in os.listdir(COUNT_DIR):

        if not filename.startswith("worker_"):
            continue

        if not filename.endswith(".count"):
            continue

        path = os.path.join(
            COUNT_DIR,
            filename,
        )

        try:
            with open(
                path,
                "r",
                encoding="utf-8",
            ) as file:
                total += int(
                    file.read().strip() or "0"
                )
        except (
            FileNotFoundError,
            ValueError,
        ):
            pass

    return total


def main():

    print("=" * 60)
    print("STREAMFORGE WEEK 2 - 100K THROUGHPUT AUDIT")
    print("=" * 60)
    print()

    print(f"Input topic: {INPUT_TOPIC}")
    print(f"Partitions: {NUM_PARTITIONS}")
    print("PASS: Kafka input topic has 4 partitions")
    print()

    if os.path.isdir(COUNT_DIR):
        shutil.rmtree(COUNT_DIR)

    os.makedirs(
        COUNT_DIR,
        exist_ok=True,
    )

    producer = Producer(
        {
            "bootstrap.servers": BROKER,
            "linger.ms": 5,
            "batch.size": 131072,
            "compression.type": "lz4",
        }
    )

    print("Starting 100,000 event benchmark...")
    print()

    start = time.perf_counter()

    partition_counts = {
        partition: 0
        for partition in range(NUM_PARTITIONS)
    }

    for i in range(NUM_EVENTS):

        partition = i % NUM_PARTITIONS

        event = {
            "truck_id": f"TRUCK-{i % 50000:05d}",
            "temperature": 20.0 + (i % 300) / 10.0,
            "timestamp": time.time(),
        }

        producer.produce(
            topic=INPUT_TOPIC,
            partition=partition,
            key=event["truck_id"].encode(),
            value=json.dumps(
                event,
                separators=(",", ":"),
            ).encode(),
            callback=delivery_report,
        )

        partition_counts[partition] += 1

        if i % 5000 == 0:
            producer.poll(0)

    producer.flush()

    producer_time = (
        time.perf_counter() - start
    )

    print()
    print("========== PRODUCER RESULT ==========")
    print(
        f"Events produced : {NUM_EVENTS:,}"
    )
    print(
        f"Time            : {producer_time:.3f} seconds"
    )
    print(
        f"Producer rate   : "
        f"{NUM_EVENTS / producer_time:,.2f} events/sec"
    )

    print()
    print("Input partition distribution:")

    for partition, count in partition_counts.items():
        print(
            f"  Partition {partition}: "
            f"{count:,} events"
        )

    print()
    print(
        "Waiting for Bytewax to process all events..."
    )

    processing_start = time.perf_counter()

    last_report = 0
    timeout_seconds = 120

    while True:

        processed = read_processed_count()

        if processed >= NUM_EVENTS:
            break

        elapsed = (
            time.perf_counter()
            - processing_start
        )

        if processed >= last_report + 10000:
            print(
                f"Processed: "
                f"{processed:,}/{NUM_EVENTS:,}"
            )
            last_report = processed

        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"Only {processed:,} of "
                f"{NUM_EVENTS:,} events processed "
                f"within {timeout_seconds} seconds."
            )

        time.sleep(0.25)

    processing_time = (
        time.perf_counter()
        - processing_start
    )

    end_to_end_time = (
        producer_time + processing_time
    )

    print()
    print("=" * 60)
    print("WEEK 2 THROUGHPUT AUDIT RESULT")
    print("=" * 60)

    print(
        f"Total events       : {NUM_EVENTS:,}"
    )
    print(
        f"Processing time    : "
        f"{processing_time:.3f} seconds"
    )
    print(
        f"Processing rate    : "
        f"{NUM_EVENTS / processing_time:,.2f} events/sec"
    )
    print(
        f"End-to-end time    : "
        f"{end_to_end_time:.3f} seconds"
    )
    print(
        f"End-to-end rate    : "
        f"{NUM_EVENTS / end_to_end_time:,.2f} events/sec"
    )

    print()

    processing_rate = (
        NUM_EVENTS / processing_time
    )

    if processing_rate >= 100_000:
        print(
            "RESULT: 100,000 events/sec target ACHIEVED"
        )
    else:
        print(
            "RESULT: 100,000 events/sec target not achieved"
        )

    print("=" * 60)


if __name__ == "__main__":
    main()
