package simulator

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/jaiswaladi246/streampulse/internal/ingestion"
)

func GenerateRunID(t time.Time, seed int64) string {
	return fmt.Sprintf("run-%s-%d", t.UTC().Format("20060102T150405.000000000Z"), seed)
}

func GenerateEvent(cfg Config, runID string, index int64, occurred time.Time) ingestion.Event {
	rng := rand.New(rand.NewSource(cfg.Seed + index))
	payload := make([]byte, cfg.PayloadSize)
	payload[0], payload[len(payload)-1] = '"', '"'
	for i := 1; i < len(payload)-1; i++ {
		payload[i] = byte('a' + rng.Intn(26))
	}
	return ingestion.Event{EventID: fmt.Sprintf("%s-%d", runID, index), EventType: cfg.EventType, Source: "simulateLoad", OccurredAtMS: occurred.UnixMilli(), SchemaVersion: 1, Payload: json.RawMessage(payload)}
}
