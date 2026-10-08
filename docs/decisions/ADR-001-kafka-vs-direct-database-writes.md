# ADR-001: Kafka Before ClickHouse Persistence

## Status

Proposed

## Context

Direct synchronous writes to analytical storage couple request availability to ClickHouse health and make burst handling difficult.

## Decision

StreamPulse will acknowledge accepted ingress requests after configured Kafka durability acknowledgement. ClickHouse persistence happens asynchronously through a consumer group.

## Consequences

- Ingress acknowledgement does not mean database visibility.
- Consumers can replay uncommitted work, so duplicate rows are possible.
- Benchmark reports must separate offered, Kafka-accepted, and ClickHouse-persisted rates.
