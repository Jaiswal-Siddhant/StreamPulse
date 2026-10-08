.PHONY: test vet fmt check help

help:
	@echo "Targets: fmt, test, vet, check"

fmt:
	go fmt ./...

test:
	go test ./...

vet:
	go vet ./...

check: fmt test vet
