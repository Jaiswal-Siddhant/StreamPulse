package simulator

import (
	"fmt"
	"time"
)

type Result struct {
	Config          Config
	RunID           string
	PlannedRequests int64
	StartedAtUTC    time.Time
	EndedAtUTC      time.Time
}

func Run(cfg Config) (Result, error) {
	if !cfg.DryRun {
		return Result{}, fmt.Errorf("Phase 0 supports configuration validation only; pass --dry-run")
	}

	started := time.Now().UTC()
	planned := PlannedRequestCount(cfg.Rate, cfg.Duration, cfg.Count)
	return Result{
		Config:          cfg,
		RunID:           GenerateRunID(started, cfg.Seed),
		PlannedRequests: planned,
		StartedAtUTC:    started,
		EndedAtUTC:      started,
	}, nil
}
