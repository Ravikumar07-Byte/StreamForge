# StreamForge

## Project Overview

**StreamForge** is a distributed real-time event processing platform designed to process high-volume truck telemetry data using **Apache Kafka** and **Bytewax**.

The system simulates telemetry generated from a large fleet of trucks and processes the incoming events through a scalable streaming pipeline. The platform is designed around continuous event ingestion, real-time filtering, transformation, event-time windowing, aggregation, and late-event handling.

The project focuses on building a reliable foundation for distributed stream processing using Python-based technologies.

### Key Capabilities

- Real-time truck telemetry generation.
- High-throughput Kafka event ingestion.
- Partitioned Kafka-based event streaming.
- Bytewax stream processing.
- Consume → Filter → Map processing topology.
- Five-minute event-time windowing.
- Per-truck telemetry aggregation.
- Average temperature calculation.
- Event-time watermark processing.
- Late-event detection and separation.
- Automated testing and throughput benchmarking.

## Problem Statement

Modern transportation systems generate large volumes of telemetry data continuously from connected vehicles. Processing this data efficiently requires a distributed streaming architecture capable of handling high event rates while maintaining correct event-time processing.

StreamForge addresses this requirement by providing a real-time telemetry processing pipeline in which truck events are produced to Kafka, consumed by the streaming layer, filtered and transformed, grouped into five-minute event-time windows, and aggregated for per-truck analysis.

The system also considers real-world streaming challenges such as late-arriving events, event-time ordering, processing performance, and future state recovery.

## Objectives

The primary objectives of StreamForge are:

- Build a distributed telemetry streaming pipeline using Apache Kafka.
- Generate realistic synthetic truck telemetry events.
- Process continuous telemetry using Bytewax.
- Implement a structured Consume → Filter → Map topology.
- Apply event-time based processing.
- Group telemetry into five-minute windows.
- Calculate per-truck average temperature.
- Handle late-arriving telemetry using watermarks and allowed lateness.
- Validate the processing pipeline through automated tests.
- Measure streaming performance against the required throughput target.
- Provide a foundation for persistent state recovery and monitoring in later development phases.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Backend and stream-processing development |
| Apache Kafka | Distributed telemetry event streaming |
| Bytewax | Real-time stream processing |
| Confluent Kafka | Python Kafka client integration |
| FastAPI | Backend API layer |
| Pydantic | Telemetry data validation |
| RocksDB / rocksdict | Planned persistent state management |
| Prometheus | Planned system monitoring and metrics |
| React / Vite | Dashboard and visualization |
| Docker | Local Kafka infrastructure |
| Pytest | Automated testing |
| GitHub Actions | Continuous integration |

## Project Scope

StreamForge is developed incrementally across multiple phases.

The initial phase establishes the Kafka-based telemetry infrastructure and producer-consumer pipeline. The stream-processing phase extends this foundation with Bytewax processing, event-time windows, aggregation, watermarking, and late-event handling.

Future phases extend the platform with persistent state, Kafka changelog recovery, fault tolerance, monitoring, and a real-time dashboard.

# System Architecture

## Overall Architecture

StreamForge follows a distributed real-time event-processing architecture designed to ingest, transport, process, aggregate, and analyze continuous truck telemetry data.

The system is divided into several major layers:

```text
Truck Telemetry Generator
          │
          ▼
     Kafka Producer
          │
          ▼
     Apache Kafka
          │
          ▼
  truck-telemetry Topic
          │
          ▼
   Bytewax Stream Processing
          │
          ├── Consume
          ├── Filter
          └── Map
          │
          ▼
    Event-Time Processing
          │
          ├── Event-Time Clock
          ├── Watermark
          └── Late-Event Handling
          │
          ▼
   Five-Minute Event-Time Window
          │
          ▼
   Per-Truck Aggregation
          │
          ▼
   Average Temperature
          │
          ▼
   Processed Stream Output
   