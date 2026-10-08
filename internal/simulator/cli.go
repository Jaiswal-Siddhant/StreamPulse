package simulator

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"
)

const Version = "0.1.0-phase0"

type Config struct {
	Rate        float64
	Duration    time.Duration
	Count       int64
	Concurrency int
	Target      string
	PayloadSize int
	Seed        int64
	DryRun      bool
	Version     bool
}

func DefaultConfig() Config {
	return Config{
		Rate:        1,
		Duration:    30 * time.Second,
		Concurrency: 1,
		Target:      "localhost:50051",
		PayloadSize: 512,
		Seed:        1,
	}
}

func Main(args []string, stdout, stderr io.Writer) int {
	cfg, err := ParseArgs(args, stdout)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 2
	}

	if cfg.Version {
		fmt.Fprintf(stdout, "StreamPulse simulateLoad %s\n", Version)
		return 0
	}

	result, err := Run(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}

	PrintTextReport(stdout, result)
	return 0
}

func ParseArgs(args []string, output io.Writer) (Config, error) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("simulateLoad", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.Usage = func() {
		fmt.Fprintln(output, "Usage: go run simulateLoad.go [flags]")
		fmt.Fprintln(output)
		fmt.Fprintln(output, "Phase 0 validates simulator configuration. Use --dry-run to print the planned run.")
		fmt.Fprintln(output)
		fs.PrintDefaults()
	}

	fs.Float64Var(&cfg.Rate, "r", cfg.Rate, "offered requests per second")
	fs.Float64Var(&cfg.Rate, "rate", cfg.Rate, "offered requests per second")
	fs.DurationVar(&cfg.Duration, "d", cfg.Duration, "run duration")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "run duration")
	fs.Int64Var(&cfg.Count, "n", cfg.Count, "optional total request cap")
	fs.Int64Var(&cfg.Count, "count", cfg.Count, "optional total request cap")
	fs.IntVar(&cfg.Concurrency, "c", cfg.Concurrency, "worker concurrency")
	fs.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "worker concurrency")
	fs.StringVar(&cfg.Target, "target", cfg.Target, "server address")
	fs.IntVar(&cfg.PayloadSize, "payload-size", cfg.PayloadSize, "payload size in bytes")
	fs.Int64Var(&cfg.Seed, "seed", cfg.Seed, "deterministic generator seed")
	fs.BoolVar(&cfg.DryRun, "dry-run", cfg.DryRun, "validate and print configuration without sending requests")
	fs.BoolVar(&cfg.Version, "version", cfg.Version, "print simulator version")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}
	return cfg, cfg.Validate()
}

func (cfg Config) Validate() error {
	if cfg.Rate <= 0 {
		return fmt.Errorf("rate must be > 0")
	}
	if cfg.Duration <= 0 {
		return fmt.Errorf("duration must be > 0")
	}
	if cfg.Count < 0 {
		return fmt.Errorf("count must be >= 0")
	}
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0")
	}
	if cfg.Target == "" {
		return fmt.Errorf("target must not be empty")
	}
	if cfg.PayloadSize < 0 {
		return fmt.Errorf("payload-size must be >= 0")
	}
	return nil
}
