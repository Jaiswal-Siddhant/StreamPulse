# Phase 2: gRPC unary ingestion

## What happens to an event

```text
simulator -> generated gRPC client -> protobuf bytes -> Publish handler
          -> shared validation -> structured log + receipt counter
          <- received=true, matching event_id, kafka_accepted=false
```

The gRPC and HTTP handlers share event validation, logs and application counters. The HTTP `/stats` endpoint includes both transports. Payload bodies are not retained; logs show metadata. Docker, Kafka and ClickHouse are still scaffolds.

## Concepts and techniques

An RPC lets a client call a remote method through a generated Go interface. Unary means one request and one response per call. Protocol Buffers define the fields and service methods in `proto/events.proto`; `protoc` generates the message serialization and client/server interfaces. The generated files are checked into the source so normal builds do not require the compiler.

The response has two separate meanings: `received` confirms validation and logging in this phase; `kafka_accepted` stays false until Kafka is integrated. Adding field 3 preserves the existing field numbers. A network failure after logging can leave the client unsure whether an event arrived. No retries or duplicate suppression are implemented yet.

gRPC status codes make failures machine-readable: `InvalidArgument` for missing fields, unsupported schema or malformed JSON; `ResourceExhausted` for oversized payloads/messages; `DeadlineExceeded` for a timed-out call; `Canceled` for cancellation; and `Unavailable` for connectivity failures. Streaming remains `Unimplemented`.

A deadline puts a limit on each request. The simulator uses a child context with `--timeout`, bounded by the overall `--duration`. Expired queued requests are dropped before sending. The server checks cancellation before recording an event, but cancellation concurrent with logging can still yield an ambiguous outcome.

Workers share a gRPC connection. The existing pacer controls offered requests per second and the bounded queue limits pending work. A successful receipt must match the event ID. Client ACK latency measures only the RPC send-to-response interval, including connection setup for the first RPC; it is not Kafka or persistence latency. Percentiles use nearest rank over at most the first 10,000 successful calls, excluding failures and dropped slots. This bounded sample is intended for local demonstrations, not sustained benchmark claims. Extremely fast calls may measure as zero at the local clock's resolution.

Reflection exposes the RPC schema to tools such as grpcurl, and the standard gRPC health service reports serving status. Counters measure requests that reach handlers. Wire decoding and gRPC message-size rejections occur before those handlers and are visible in client error statistics, not `/stats.rejected`.

## Run and inspect

Start in one terminal:

```powershell
go run ./cmd/ingest
```

In another:

```powershell
go run simulateLoad.go --transport grpc --target localhost:50051 --count 100 --rate 10 --workers 4 --timeout 5s
Invoke-RestMethod http://127.0.0.1:8080/stats
go run simulateLoad.go --transport http --count 10 --rate 10
```

Stop the terminal server with Ctrl+C. For a background executable, find its PID using `Get-Process ingest` and stop that specific PID with `Stop-Process -Id <PID>` (forced termination). Graceful shutdown drains current requests for up to five seconds.

## Independent grpcurl smoke tests

Install the pinned tool with `go install github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.3`, then ensure your Go bin directory is on PATH. This project's local copy is `.cache/bin/grpcurl.exe`.

```powershell
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -d '{}' localhost:50051 grpc.health.v1.Health/Check
'{"event":{"eventId":"manual-1","eventType":"page_view","source":"grpcurl","occurredAtMs":"1791450000000","schemaVersion":1,"payloadJson":"e30="}}' | grpcurl -plaintext -d '@' localhost:50051 streampulse.v1.EventService/Publish
grpcurl -plaintext -d '{}' localhost:50051 streampulse.v1.EventService/Publish
```

`payloadJson` is a protobuf bytes field: its JSON representation is base64. `e30=` decodes to `{}`. Expected: the valid event returns a matching ID and `received: true`; the empty request returns `InvalidArgument`. Protobuf JSON omits default-valued fields, so `kafkaAccepted: false` may be absent from grpcurl output.

## Verify and regenerate

```powershell
go test ./...
go vet ./...
```

Integration tests use real local TCP gRPC servers, validate 100 concurrent-worker receipts and health, assert error codes and byte limits, and exercise request deadlines, canceled contexts, unsupported streaming and CLI exit codes. HTTP regression tests remain active.

To regenerate, use protoc 32.1, `protoc-gen-go` 1.36.9 and `protoc-gen-go-grpc` 1.5.1 on PATH:

```powershell
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.9
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
protoc --go_out=. --go_opt=module=github.com/jaiswaladi246/streampulse --go-grpc_out=. --go-grpc_opt=module=github.com/jaiswaladi246/streampulse proto/events.proto
```

`make proto` runs the same compiler command. Go dependency versions are pinned in go.mod/go.sum. Reference: [gRPC Go quick start](https://grpc.io/docs/languages/go/quickstart/) and [gRPC deadlines](https://grpc.io/docs/guides/deadlines/).

Phase 3 will add ClickHouse inserts and per-run stored-event verification. This directory has no Git repository, so milestone commits/tags have not been created.

## Observed verification on 2026-10-08

`go test ./...`, `go vet ./...` and the executable build passed with Go 1.26.2 on windows/386. grpcurl listed EventService, returned health SERVING, acknowledged `manual-phase2-1` and rejected an empty request with InvalidArgument.

| Transport | Offered requests/s | Planned | Sent | Received | Failed | Dropped | CLI exit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| gRPC, localhost | 100 | 100 | 86 | 86 | 0 | 14 | 1 |
| gRPC, 127.0.0.1 | 10 | 100 | 100 | 100 | 0 | 0 | 0 |
| HTTP | 10 | 10 | 10 | 10 | 0 | 0 | 0 |

The initial faster live trial demonstrated bounded-queue drops under local timing conditions, not an event-loss or throughput claim. The slow gRPC demo had client ACK p50=0s, p95=542.1us and p99=609.6us; zero durations reflect timer resolution on this environment, not zero-cost networking. The listed demos and manual grpcurl event account for 197 receipts. Another run (`run-20261008T100245.529511400Z-1`) appeared in server logs with 100 additional receipts; its client report was not captured by this verification. Final server counters were received=297, rejected=1. Raw run IDs and counts are in `benchmarks/results/phase2-local.json`. The demo server was stopped after verification.
