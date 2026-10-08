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
