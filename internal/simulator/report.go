package simulator

import (
	"fmt"
	"io"
)

func PrintTextReport(w io.Writer, result Result) {
	cfg := result.Config
	fmt.Fprintln(w, "StreamPulse simulateLoad dry run")
	fmt.Fprintf(w, "run_id: %s\n", result.RunID)
	fmt.Fprintf(w, "target: %s\n", cfg.Target)
	fmt.Fprintf(w, "requested_rps: %.2f\n", cfg.Rate)
	fmt.Fprintf(w, "duration: %s\n", cfg.Duration)
	fmt.Fprintf(w, "count_cap: %d\n", cfg.Count)
	fmt.Fprintf(w, "planned_requests: %d\n", result.PlannedRequests)
	fmt.Fprintf(w, "concurrency: %d\n", cfg.Concurrency)
	fmt.Fprintf(w, "payload_size_bytes: %d\n", cfg.PayloadSize)
	fmt.Fprintf(w, "seed: %d\n", cfg.Seed)
}
