# Phase 1 verification

Phase 1 implements a local HTTP receive-and-log API. The protobuf contract remains a draft for Phase 2. No Kafka acceptance or persistence is claimed by an HTTP receipt.

## Reproduce

```powershell
go test ./...
go vet ./...
go run ./cmd/ingest
```

In another terminal:

```powershell
go run simulateLoad.go -r 10 -n 10
go run simulateLoad.go -r 10 -n 100 --seed 42
Invoke-RestMethod http://127.0.0.1:8080/stats
```

Expected: each run reports exact sent/received counts and zero failures/drops. Fresh server totals: 110 received, zero rejected. Integration tests also use an isolated HTTP server to reconcile 10 and 100 receipts and assert malformed/oversized requests never increment received. Invalid responses produce a nonzero CLI exit.

This is a correctness demonstration, not a performance benchmark. Phase 2 adds generated protobuf clients, gRPC unary transport and explicit RPC error semantics. Kafka durability arrives in Phase 4.

## Observed on 2026-10-08

`go test ./...` and `go vet ./...` passed on Windows with Go 1.26.2. Live runs at 10 requests/second produced:

| Requests | Sent | Received | Failed | Dropped |
| --- | --- | --- | --- | --- |
| 10 | 10 | 10 | 0 | 0 |
| 100 | 100 | 100 | 0 | 0 |

The live server returned `{"received":110,"rejected":0}`. Raw counts and run identifiers are recorded in `benchmarks/results/phase1-local.json`. The working directory has no Git repository, so no milestone commit or tag was created.
