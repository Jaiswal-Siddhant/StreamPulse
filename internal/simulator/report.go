package simulator

import (
	"fmt"
	"io"
)

func PrintTextReport(w io.Writer, result Result) {
	cfg := result.Config
	if cfg.DryRun {
		fmt.Fprintln(w, "StreamPulse simulateLoad dry run")
	} else {
		fmt.Fprintln(w, "StreamPulse simulateLoad HTTP run")
	}
	fmt.Fprintf(w, "run_id: %s\n", result.RunID)
	fmt.Fprintf(w, "target: %s\n", cfg.Target)
	fmt.Fprintf(w, "requested_rps: %.2f\n", cfg.Rate)
	fmt.Fprintf(w, "duration: %s\n", cfg.Duration)
	fmt.Fprintf(w, "count_cap: %d\n", cfg.Count)
	fmt.Fprintf(w, "planned_requests: %d\n", result.PlannedRequests)
	fmt.Fprintf(w, "concurrency: %d\n", cfg.Concurrency)
	fmt.Fprintf(w, "payload_size_bytes: %d\n", cfg.PayloadSize)
	fmt.Fprintf(w, "seed: %d\n", cfg.Seed)
	fmt.Fprintf(w, "event_type: %s\n", cfg.EventType)
	if !cfg.DryRun {
		fmt.Fprintf(w, "sent: %d\nreceived: %d\nfailed: %d\ndropped_before_send: %d\n", result.Sent, result.Received, result.Failed, result.Dropped)
		if result.FirstError != "" {
			fmt.Fprintf(w, "first_error: %s\n", result.FirstError)
		}
	}
}
