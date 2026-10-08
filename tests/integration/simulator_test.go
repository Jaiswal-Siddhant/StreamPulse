package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaiswaladi246/streampulse/internal/ingestion"
	"github.com/jaiswaladi246/streampulse/internal/simulator"
)

func TestSimulatorReceipts(t *testing.T) {
	for _, count := range []int64{10, 100} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ingress := ingestion.NewServer(ingestion.DefaultMaxPayload, slog.New(slog.NewTextHandler(io.Discard, nil)))
			server := httptest.NewServer(ingress.Handler())
			defer server.Close()
			cfg := simulator.DefaultConfig()
			cfg.Transport = "http"
			cfg.Target = server.URL
			cfg.Count = count
			cfg.Rate = 100
			cfg.Concurrency = 4
			cfg.Duration = 3 * time.Second
			result, err := simulator.Run(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if result.Sent != count || result.Received != count || result.Failed != 0 || result.Dropped != 0 || ingress.Stats().Received != uint64(count) {
				t.Fatalf("result=%+v stats=%+v", result, ingress.Stats())
			}
		})
	}
}

func TestIngressRejections(t *testing.T) {
	ingress := ingestion.NewServer(64, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewServer(ingress.Handler())
	defer server.Close()
	cfg := simulator.DefaultConfig()
	cfg.PayloadSize = 64
	valid := simulator.GenerateEvent(cfg, "test", 1, time.Now())
	missing := valid
	missing.EventID = ""
	null := valid
	null.Payload = json.RawMessage("null")
	oversized := valid
	cfg.PayloadSize = 65
	oversized.Payload = simulator.GenerateEvent(cfg, "test", 2, time.Now()).Payload
	largeBody := valid
	cfg.PayloadSize = 5000
	largeBody.Payload = simulator.GenerateEvent(cfg, "test", 3, time.Now()).Payload
	marshal := func(event ingestion.Event) []byte {
		b, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cases := []struct {
		name   string
		body   []byte
		status int
	}{
		{"valid boundary", marshal(valid), 200}, {"missing ID", marshal(missing), 400}, {"null payload", marshal(null), 400}, {"oversize", marshal(oversized), 413}, {"malformed", []byte("{"), 400}, {"trailing event", append(marshal(valid), marshal(valid)...), 400}, {"body limit", bytes.Repeat([]byte("x"), 5000), 400},
		{"large JSON body", marshal(largeBody), 413},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response, err := http.Post(server.URL+"/events", "application/json", bytes.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != tc.status {
				b, _ := io.ReadAll(response.Body)
				t.Fatalf("status=%d want=%d body=%s", response.StatusCode, tc.status, b)
			}
		})
	}
	if got := ingress.Stats(); got.Received != 1 || got.Rejected != 7 {
		t.Fatalf("stats=%+v", got)
	}
}

func TestSimulatorFailure(t *testing.T) {
	ingress := ingestion.NewServer(8, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewServer(ingress.Handler())
	defer server.Close()
	var out, stderr bytes.Buffer
	code := simulator.Main([]string{"--transport", "http", "--target", server.URL, "-n", "1", "-r", "10"}, &out, &stderr)
	if code != 1 || ingress.Stats().Rejected != 1 {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, &out, &stderr)
	}
}
