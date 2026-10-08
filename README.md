# StreamPulse

StreamPulse is a self-hosted event ingestion pipeline built in phases. The target architecture is:

```text
load generator -> gRPC ingestion -> Kafka -> consumer -> ClickHouse
```

The current repository is Phase 0: project structure, CLI skeleton, documentation, and automated checks. In this phase no external services are required.

## Quick Start

```powershell
go test ./...
go vet ./...
go run simulateLoad.go --help
go run simulateLoad.go --dry-run -r 10 -n 100
```

`-r` / `--rate` means offered requests per second. A successful ingress acknowledgement in later phases will mean durable Kafka acceptance, not ClickHouse persistence.

## Phase 0 Scope

- Go module and package layout.
- Root `simulateLoad.go` wrapper backed by `internal/simulator`.
- Compatibility entrypoints under `cmd/loadgen` and `cmd/simulateLoad`.
- Docker Compose scaffold for future Kafka, ClickHouse, Prometheus, and Grafana services.
- Protobuf contract draft.
- CI skeleton for format, tests, and vet.

## Commands

```powershell
go run simulateLoad.go --help
go run simulateLoad.go --version
go run simulateLoad.go --dry-run --rate 100 --duration 30s --count 1000 --concurrency 4
```

## Repository Layout

```text
cmd/ingest/                  # future gRPC server
cmd/consumer/                # future Kafka to ClickHouse worker
cmd/loadgen/                 # loadgen compatibility entrypoint
cmd/simulateLoad/            # package-form simulator entrypoint
internal/ingestion/          # future validation and handlers
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
