package simulator

import (
	"fmt"
	"io"
	"sort"
)

func PrintTextReport(w io.Writer, result Result) {
	cfg := result.Config
	if cfg.DryRun {
		fmt.Fprintln(w, "StreamPulse simulateLoad dry run")
	} else {
		fmt.Fprintf(w, "StreamPulse simulateLoad %s run\n", cfg.Transport)
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
	fmt.Fprintf(w, "transport: %s\ntimeout: %s\n", cfg.Transport, cfg.Timeout)
	if !cfg.DryRun {
		fmt.Fprintf(w, "sent: %d\nreceived: %d\nfailed: %d\ndropped_before_send: %d\n", result.Sent, result.Received, result.Failed, result.Dropped)
		fmt.Fprintf(w, "ack_latency_samples: %d\nack_p50: %s\nack_p95: %s\nack_p99: %s\n", result.AckSamples, result.AckP50, result.AckP95, result.AckP99)
		keys := make([]string, 0, len(result.ErrorTypes))
		for key := range result.ErrorTypes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(w, "error_%s: %d\n", key, result.ErrorTypes[key])
		}
		if result.FirstError != "" {
			fmt.Fprintf(w, "first_error: %s\n", result.FirstError)
		}
	}
}
