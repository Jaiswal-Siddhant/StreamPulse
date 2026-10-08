package simulator

import (
	"bytes"
	"strings"
	"testing"
)

func TestHelpExitsSuccessfully(t *testing.T) {
	var out, errOut bytes.Buffer
	code := Main([]string{"--help"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d with stderr %q", code, errOut.String())
	}
	if !strings.Contains(out.String(), "Usage: go run simulateLoad.go") {
		t.Fatalf("help output missing usage: %q", out.String())
	}
}

func TestDryRunReportsPlannedRequests(t *testing.T) {
	var out, errOut bytes.Buffer
	code := Main([]string{"--dry-run", "-r", "10", "-n", "25", "--duration", "10s"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d with stderr %q", code, errOut.String())
	}
	if !strings.Contains(out.String(), "planned_requests: 25") {
		t.Fatalf("dry-run output missing capped planned requests: %q", out.String())
	}
}

func TestValidationRejectsInvalidRate(t *testing.T) {
	var out, errOut bytes.Buffer
	code := Main([]string{"--dry-run", "--rate", "0"}, &out, &errOut)
	if code != 2 {
		t.Fatalf("expected exit 2, got %d", code)
	}
	if !strings.Contains(errOut.String(), "rate must be > 0") {
		t.Fatalf("stderr missing validation error: %q", errOut.String())
	}
}

func TestTransportFlags(t *testing.T) {
	cfg, err := ParseArgs([]string{"--transport", "grpc", "--target", "localhost:50051", "--workers", "10", "--timeout", "2s"}, &bytes.Buffer{})
	if err != nil || cfg.Concurrency != 10 || cfg.Timeout.String() != "2s" {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
	cfg, err = ParseArgs([]string{"--transport", "http"}, &bytes.Buffer{})
	if err != nil || cfg.Target != "http://127.0.0.1:8080" {
		t.Fatalf("HTTP cfg=%+v err=%v", cfg, err)
	}
	for _, args := range [][]string{{"--transport", "invalid"}, {"--timeout", "0s"}, {"--target", "http://localhost:50051"}, {"--target", "example.com:50051"}, {"--workers", "0"}} {
		if _, err := ParseArgs(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted invalid args: %v", args)
		}
	}
}
