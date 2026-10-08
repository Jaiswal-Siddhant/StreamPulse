package simulator

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestGeneratorDeterministic(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	a := GenerateEvent(cfg, "run", 1, time.Unix(1, 0))
	b := GenerateEvent(cfg, "run", 1, time.Unix(1, 0))
	if a.EventID != b.EventID || !bytes.Equal(a.Payload, b.Payload) || !json.Valid(a.Payload) || len(a.Payload) != cfg.PayloadSize {
		t.Fatal("generator must reproduce valid, exact-size payloads")
	}
	c := GenerateEvent(cfg, "run", 2, time.Unix(1, 0))
	if a.EventID == c.EventID || bytes.Equal(a.Payload, c.Payload) {
		t.Fatal("event index must distinguish events")
	}
}
