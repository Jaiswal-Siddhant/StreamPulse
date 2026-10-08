package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaiswaladi246/streampulse/internal/ingestion"
	pb "github.com/jaiswaladi246/streampulse/proto/streampulsev1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
	ErrorTypes      map[string]int64
	AckP50          time.Duration
	AckP95          time.Duration
	AckP99          time.Duration
	AckSamples      int
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
	client := &http.Client{Transport: transport, Timeout: cfg.Timeout, CheckRedirect: func(r *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	send := func(ctx context.Context, event ingestion.Event) error { return publish(ctx, client, cfg.Target, event) }
	if cfg.Transport == "grpc" {
		conn, err := grpc.NewClient(cfg.Target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(16*1024*1024+4096)))
		if err != nil {
			return Result{}, err
		}
		defer conn.Close()
		rpc := pb.NewEventServiceClient(conn)
		send = func(ctx context.Context, event ingestion.Event) error {
			response, err := rpc.Publish(ctx, &pb.PublishRequest{Event: &pb.Event{EventId: event.EventID, EventType: event.EventType, Source: event.Source, OccurredAtMs: event.OccurredAtMS, SchemaVersion: event.SchemaVersion, PayloadJson: event.Payload, UserId: event.UserID}})
			if err != nil {
				return err
			}
			if !response.Received || response.EventId != event.EventID {
				return fmt.Errorf("invalid receipt for %s", event.EventID)
			}
			return nil
		}
	}
	jobs := make(chan int64, cfg.Concurrency)
	var sent, received, failed atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, 10000)
	result.ErrorTypes = make(map[string]int64)
	for worker := 0; worker < cfg.Concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if ctx.Err() != nil {
					continue
				}
				event := GenerateEvent(cfg, result.RunID, index, time.Now())
				sent.Add(1)
				requestCtx, requestCancel := context.WithTimeout(ctx, cfg.Timeout)
				start := time.Now()
				err := send(requestCtx, event)
				elapsed := time.Since(start)
				requestCancel()
				mu.Lock()
				if err != nil {
					failed.Add(1)
					if result.FirstError == "" {
						result.FirstError = err.Error()
					}
					result.ErrorTypes[status.Code(err).String()]++
				} else {
					received.Add(1)
					if len(latencies) < cap(latencies) {
						latencies = append(latencies, elapsed)
					}
				}
				mu.Unlock()
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
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	result.AckSamples = len(latencies)
	if len(latencies) > 0 {
		result.AckP50 = percentile(latencies, 50)
		result.AckP95 = percentile(latencies, 95)
		result.AckP99 = percentile(latencies, 99)
	}
	return result, nil
}

func percentile(samples []time.Duration, p int) time.Duration {
	return samples[(len(samples)*p+99)/100-1]
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
