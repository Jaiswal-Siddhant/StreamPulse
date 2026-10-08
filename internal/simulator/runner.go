package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaiswaladi246/streampulse/internal/ingestion"
)

type Result struct {
	Config          Config
	RunID           string
	PlannedRequests int64
	StartedAtUTC    time.Time
	EndedAtUTC      time.Time
	Sent            int64
	Received        int64
	Failed          int64
	Dropped         int64
	FirstError      string
}

func Run(cfg Config) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, err
	}
	started := time.Now()
	result := Result{Config: cfg, RunID: GenerateRunID(started, cfg.Seed), PlannedRequests: PlannedRequestCount(cfg.Rate, cfg.Duration, cfg.Count), StartedAtUTC: started.UTC(), EndedAtUTC: started.UTC()}
	if cfg.DryRun {
		return result, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()
	transport := &http.Transport{MaxConnsPerHost: cfg.Concurrency, MaxIdleConnsPerHost: cfg.Concurrency}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(r *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	jobs := make(chan int64, cfg.Concurrency)
	var sent, received, failed atomic.Int64
	var wg sync.WaitGroup
	var first sync.Once
	for worker := 0; worker < cfg.Concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				event := GenerateEvent(cfg, result.RunID, index, time.Now())
				sent.Add(1)
				if err := publish(ctx, client, cfg.Target, event); err != nil {
					failed.Add(1)
					first.Do(func() { result.FirstError = err.Error() })
				} else {
					received.Add(1)
				}
			}
		}()
	}
	// Absolute scheduling avoids ticker drift; the bounded queue drops excess slots.
schedule:
	for index := int64(0); index < result.PlannedRequests; index++ {
		at := started.Add(time.Duration(float64(index) / cfg.Rate * float64(time.Second)))
		timer := time.NewTimer(time.Until(at))
		select {
		case <-ctx.Done():
			timer.Stop()
			break schedule
		case <-timer.C:
		}
		select {
		case <-ctx.Done():
			break schedule
		case jobs <- index:
		default:
		}
	}
	close(jobs)
	wg.Wait()
	result.Sent, result.Received, result.Failed = sent.Load(), received.Load(), failed.Load()
	result.Dropped = result.PlannedRequests - result.Sent
	result.EndedAtUTC = time.Now().UTC()
	return result, nil
}

func publish(ctx context.Context, client *http.Client, target string, event ingestion.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(target, "/")+"/events", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(detail)))
	}
	var receipt ingestion.Receipt
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&receipt); err != nil {
		return err
	}
	if !receipt.Received || receipt.EventID != event.EventID {
		return fmt.Errorf("invalid receipt for %s", event.EventID)
	}
	return nil
}
