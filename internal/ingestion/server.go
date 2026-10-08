package ingestion

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
)

const DefaultMaxPayload = 64 * 1024

type Event struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	Source        string          `json:"source"`
	OccurredAtMS  int64           `json:"occurred_at_ms"`
	SchemaVersion uint32          `json:"schema_version"`
	Payload       json.RawMessage `json:"payload_json"`
	UserID        string          `json:"user_id,omitempty"`
}

type Receipt struct {
	EventID  string `json:"event_id"`
	Received bool   `json:"received"`
}

type Stats struct {
	Received uint64 `json:"received"`
	Rejected uint64 `json:"rejected"`
}

type Server struct {
	maxPayload int
	logger     *slog.Logger
	received   atomic.Uint64
	rejected   atomic.Uint64
}

func NewServer(maxPayload int, logger *slog.Logger) *Server {
	return &Server{maxPayload: maxPayload, logger: logger}
}

func (s *Server) Stats() Stats {
	return Stats{Received: s.received.Load(), Rejected: s.rejected.Load()}
}

func Validate(event Event, maxPayload int) error {
	if strings.TrimSpace(event.EventID) == "" || strings.TrimSpace(event.EventType) == "" || strings.TrimSpace(event.Source) == "" {
		return errors.New("event_id, event_type and source are required")
	}
	if len(event.EventID) > 256 || len(event.EventType) > 256 || len(event.Source) > 256 {
		return errors.New("event_id, event_type and source must be at most 256 bytes")
	}
	if len(event.UserID) > 256 {
		return errors.New("user_id must be at most 256 bytes")
	}
	if event.OccurredAtMS <= 0 || event.SchemaVersion != 1 {
		return errors.New("occurred_at_ms must be positive and schema_version must be 1")
	}
	if len(event.Payload) > maxPayload {
		return errors.New("payload exceeds configured byte limit")
	}
	if !json.Valid(event.Payload) || strings.TrimSpace(string(event.Payload)) == "null" {
		return errors.New("payload_json must contain a non-null JSON value")
	}
	return nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, s.Stats()) })
	mux.HandleFunc("POST /events", s.publish)
	return mux
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, int64(s.maxPayload)*2+4096)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var event Event
	err := decoder.Decode(&event)
	if err == nil {
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			err = errors.New("request must contain exactly one JSON event")
		}
	}
	code := http.StatusBadRequest
	var oversized *http.MaxBytesError
	if errors.As(err, &oversized) {
		code = http.StatusRequestEntityTooLarge
	}
	if err == nil && len(event.Payload) > s.maxPayload {
		err = errors.New("payload exceeds configured byte limit")
		code = http.StatusRequestEntityTooLarge
	}
	if err == nil {
		err = Validate(event, s.maxPayload)
	}
	if err != nil {
		s.rejected.Add(1)
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	s.record(event)
	writeJSON(w, http.StatusOK, Receipt{EventID: event.EventID, Received: true})
}

func (s *Server) record(event Event) {
	s.logger.Info("event received", "event_id", event.EventID, "event_type", event.EventType, "source", event.Source, "payload_bytes", len(event.Payload))
	s.received.Add(1)
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
