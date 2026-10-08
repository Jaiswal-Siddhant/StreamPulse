package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaiswaladi246/streampulse/internal/ingestion"
)

func main() {
	address := flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
	grpcAddress := flag.String("grpc-listen", "127.0.0.1:50051", "local plaintext gRPC listen address")
	maxPayload := flag.Int("max-payload-size", ingestion.DefaultMaxPayload, "maximum payload bytes")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if *maxPayload < 2 || *maxPayload > 16*1024*1024 {
		logger.Error("max-payload-size must be between 2 and 16777216")
		os.Exit(2)
	}
	host, _, err := net.SplitHostPort(*grpcAddress)
	ip := net.ParseIP(host)
	if err != nil || (host != "localhost" && (ip == nil || !ip.IsLoopback())) {
		logger.Error("plaintext gRPC listen address must be loopback")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ingress := ingestion.NewServer(*maxPayload, logger)
	server := &http.Server{Addr: *address, Handler: ingress.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	httpListener, err := net.Listen("tcp", *address)
	if err != nil {
		logger.Error("HTTP bind failed", "error", err)
		os.Exit(1)
	}
	grpcListener, err := net.Listen("tcp", *grpcAddress)
	if err != nil {
		_ = httpListener.Close()
		logger.Error("gRPC bind failed", "error", err)
		os.Exit(1)
	}
	rpcServer := ingestion.NewGRPCServer(ingress)
	done := make(chan error, 2)
	go func() { done <- server.Serve(httpListener) }()
	go func() { done <- rpcServer.Serve(grpcListener) }()
	logger.Info("ingress ready", "http", *address, "grpc", *grpcAddress, "max_payload_bytes", *maxPayload)
	failed := false
	select {
	case err := <-done:
		logger.Error("server stopped unexpectedly", "error", err)
		failed = true
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	grpcStopped := make(chan struct{})
	go func() { rpcServer.GracefulStop(); close(grpcStopped) }()
	if err := server.Shutdown(shutdown); err != nil {
		logger.Error("HTTP shutdown failed", "error", err)
		_ = server.Close()
	}
	select {
	case <-grpcStopped:
	case <-shutdown.Done():
		rpcServer.Stop()
	}
	logger.Info("ingress stopped", "counts", ingress.Stats())
	if failed {
		os.Exit(1)
	}
}
