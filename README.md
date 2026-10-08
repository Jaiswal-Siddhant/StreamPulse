# StreamPulse

StreamPulse is a self-hosted event ingestion pipeline built in phases. The target architecture is:

```text
load generator -> gRPC ingestion -> Kafka -> consumer -> ClickHouse
```

The current repository is Phase 1: a local HTTP ingestion server and a synthetic event simulator. No external services are required.

## Quick Start

```powershell
go test ./...
go vet ./...
go run simulateLoad.go --help
go run simulateLoad.go --dry-run -r 10 -n 100
```

Start the server in one terminal:

```powershell
go run ./cmd/ingest
```

Send events from another terminal:

```powershell
go run simulateLoad.go -r 10 -n 10
go run simulateLoad.go -r 10 -n 100 --event-type page_view --payload-size 512 --seed 42
Invoke-RestMethod http://127.0.0.1:8080/stats
```

A fresh server reports `received: 110` and `rejected: 0` after these two runs. The server listens on loopback by default. Stop it with Ctrl+C. Use `--listen` and `--max-payload-size` to configure the server.

HTTP endpoints: `POST /events`, `GET /healthz`, and `GET /stats`. Events require `event_id`, `event_type`, `source`, a positive `occurred_at_ms`, `schema_version: 1`, and a non-null JSON `payload_json`. The default payload limit is 65,536 bytes; invalid events return HTTP 400 and oversized payloads return HTTP 413. Identifier fields are limited to 256 bytes.

Phase 1 receipts confirm validation and logging only. Events are not durably stored, counters reset on restart, and duplicates count as separate receipts. gRPC, Kafka and ClickHouse are later phases.

`--target` now defaults to `http://127.0.0.1:8080`. Payload size includes the quotes around the generated JSON string and must be at least 2 bytes. Seed and event index deterministically generate payloads; IDs include a unique run timestamp and index. The simulator uses absolute pacing, bounded workers and a queue of at most `--concurrency` events. It reports sent, received, failed and dropped-before-send counts and exits nonzero for any failure or drop. Requests have a 5-second timeout and the run duration is a hard deadline. A failed response can be ambiguous if the server received the event before a connection failed; no automatic retries occur.

`-r` / `--rate` means offered requests per second. A successful ingress acknowledgement in later phases will mean durable Kafka acceptance, not ClickHouse persistence.

## Delivered Scope

- Go module and package layout.
- Root `simulateLoad.go` wrapper backed by `internal/simulator`.
- Compatibility entrypoints under `cmd/loadgen` and `cmd/simulateLoad`.
- Docker Compose scaffold for future Kafka, ClickHouse, Prometheus, and Grafana services.
- Protobuf contract draft.
- CI skeleton for format, tests, and vet.
- HTTP ingestion with validation, structured metadata logs, health checks and receipt counters.
- Paced synthetic traffic, deterministic payloads, bounded concurrency and failure reports.
- Local integration tests for 10/100 receipts, rejection cases and CLI failures.

## Commands

```powershell
go run simulateLoad.go --help
go run simulateLoad.go --version
go run simulateLoad.go --dry-run --rate 100 --duration 30s --count 1000 --concurrency 4
```

## Repository Layout

```text
cmd/ingest/                  # Phase 1 HTTP server; future gRPC server
cmd/consumer/                # future Kafka to ClickHouse worker
cmd/loadgen/                 # loadgen compatibility entrypoint
cmd/simulateLoad/            # package-form simulator entrypoint
internal/ingestion/          # event validation and HTTP handlers
internal/kafka/              # future Kafka adapters
internal/metrics/            # future instrumentation
internal/retry/              # future retry and DLQ policy
internal/simulator/          # CLI, pacing, generation, reporting
internal/storage/            # future ClickHouse access
proto/                       # protobuf source
migrations/clickhouse/       # ClickHouse migrations
deployments/docker/          # local infrastructure
benchmarks/scenarios/        # versioned workloads
benchmarks/results/          # run evidence
docs/decisions/              # ADRs
```

## Guardrails

Performance goals are targets, not claims. Benchmark reports must distinguish offered rate, accepted rate, Kafka acknowledgement latency, persisted rate, and end-to-end ClickHouse-visible latency.
