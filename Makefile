.PHONY: test vet fmt check help proto

help:
	@echo "Targets: fmt, test, vet, check, proto"

fmt:
	go fmt ./...

test:
	go test ./...

vet:
	go vet ./...

check: fmt test vet

proto:
	protoc --go_out=. --go_opt=module=github.com/jaiswaladi246/streampulse --go-grpc_out=. --go-grpc_opt=module=github.com/jaiswaladi246/streampulse proto/events.proto
