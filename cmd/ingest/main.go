package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/jaiswaladi246/streampulse/internal/ingestion"
)

func main() {
	address := flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
	maxPayload := flag.Int("max-payload-size", ingestion.DefaultMaxPayload, "maximum payload bytes")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if *maxPayload < 2 || *maxPayload > 16*1024*1024 {
		logger.Error("max-payload-size must be between 2 and 16777216")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ingress := ingestion.NewServer(*maxPayload, logger)
	server := &http.Server{Addr: *address, Handler: ingress.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	logger.Info("starting HTTP ingress", "listen", *address, "max_payload_bytes", *maxPayload)
	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			logger.Error("shutdown failed", "error", err)
			_ = server.Close()
		}
	}
	logger.Info("ingress stopped", "counts", ingress.Stats())
}
