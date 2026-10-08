package integration

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/jaiswaladi246/streampulse/internal/ingestion"
	"github.com/jaiswaladi246/streampulse/internal/simulator"
	pb "github.com/jaiswaladi246/streampulse/proto/streampulsev1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func startGRPC(t *testing.T, server *grpc.Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	return listener.Addr().String()
}

func connectGRPC(t *testing.T, target string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestGRPCReceipts(t *testing.T) {
	ingress := ingestion.NewServer(ingestion.DefaultMaxPayload, slog.New(slog.NewTextHandler(io.Discard, nil)))
	target := startGRPC(t, ingestion.NewGRPCServer(ingress))
	conn := connectGRPC(t, target)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	health, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: "streampulse.v1.EventService"})
	if err != nil || health.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("health=%v err=%v", health, err)
	}
	response, err := pb.NewEventServiceClient(conn).Publish(ctx, &pb.PublishRequest{Event: validProtoEvent()})
	if err != nil || !response.GetReceived() || response.GetKafkaAccepted() || response.GetEventId() != "test-1" {
		t.Fatalf("receipt=%v err=%v", response, err)
	}
	cfg := simulator.DefaultConfig()
	cfg.Target = target
	cfg.Count = 100
	cfg.Rate = 100
	cfg.Concurrency = 8
	cfg.Duration = 3 * time.Second
	result, err := simulator.Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Sent != 100 || result.Received != 100 || result.Failed != 0 || result.Dropped != 0 || result.AckSamples != 100 || ingress.Stats().Received != 101 {
		t.Fatalf("result=%+v stats=%+v", result, ingress.Stats())
	}
	if result.AckP50 < 0 || result.AckP50 > result.AckP95 || result.AckP95 > result.AckP99 {
		t.Fatalf("invalid latency percentiles: %+v", result)
	}
}

func validProtoEvent() *pb.Event {
	return &pb.Event{EventId: "test-1", EventType: "page_view", Source: "test", OccurredAtMs: time.Now().UnixMilli(), SchemaVersion: 1, PayloadJson: []byte(`{"page":"home"}`)}
}

func TestGRPCValidation(t *testing.T) {
	ingress := ingestion.NewServer(64, slog.New(slog.NewTextHandler(io.Discard, nil)))
	client := pb.NewEventServiceClient(connectGRPC(t, startGRPC(t, ingestion.NewGRPCServer(ingress))))
	cases := []struct {
		name   string
		mutate func(*pb.Event)
		code   codes.Code
	}{
		{"missing ID", func(e *pb.Event) { e.EventId = "" }, codes.InvalidArgument},
		{"missing source", func(e *pb.Event) { e.Source = "" }, codes.InvalidArgument},
		{"schema", func(e *pb.Event) { e.SchemaVersion = 2 }, codes.InvalidArgument},
		{"timestamp", func(e *pb.Event) { e.OccurredAtMs = 0 }, codes.InvalidArgument},
		{"JSON", func(e *pb.Event) { e.PayloadJson = []byte("{") }, codes.InvalidArgument},
		{"null", func(e *pb.Event) { e.PayloadJson = []byte("null") }, codes.InvalidArgument},
		{"oversize", func(e *pb.Event) { e.PayloadJson = append(append([]byte{'"'}, bytes.Repeat([]byte{'a'}, 63)...), '"') }, codes.ResourceExhausted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := validProtoEvent()
			tc.mutate(event)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := client.Publish(ctx, &pb.PublishRequest{Event: event})
			if status.Code(err) != tc.code {
				t.Fatalf("error=%v want=%v", err, tc.code)
			}
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.Publish(ctx, &pb.PublishRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing event: %v", err)
	}
	if got := ingress.Stats(); got.Received != 0 || got.Rejected != 8 {
		t.Fatalf("stats=%+v", got)
	}
	_, err = client.Publish(ctx, &pb.PublishRequest{Event: &pb.Event{PayloadJson: bytes.Repeat([]byte{'a'}, 5000)}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("transport limit: %v", err)
	}
	// Transport-level decoding/size failures never reach the application counter.
	if ingress.Stats().Rejected != 8 {
		t.Fatal("transport rejection reached handler")
	}
	stream, err := client.PublishStream(ctx)
	if err == nil {
		_, err = stream.CloseAndRecv()
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("streaming: %v", err)
	}
}

type slowService struct {
	pb.UnimplementedEventServiceServer
}

func (slowService) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func TestGRPCDeadlineAndCLIExit(t *testing.T) {
	server := grpc.NewServer()
	pb.RegisterEventServiceServer(server, slowService{})
	target := startGRPC(t, server)
	var out, stderr bytes.Buffer
	code := simulator.Main([]string{"--target", target, "--timeout", "50ms", "--count", "1"}, &out, &stderr)
	if code != 1 || !bytes.Contains(out.Bytes(), []byte("error_DeadlineExceeded: 1")) {
		t.Fatalf("code=%d out=%s stderr=%s", code, &out, &stderr)
	}
}

func TestGRPCCancelledRequest(t *testing.T) {
	ingress := ingestion.NewServer(64, slog.New(slog.NewTextHandler(io.Discard, nil)))
	client := pb.NewEventServiceClient(connectGRPC(t, startGRPC(t, ingestion.NewGRPCServer(ingress))))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Publish(ctx, &pb.PublishRequest{Event: validProtoEvent()})
	if status.Code(err) != codes.Canceled || ingress.Stats().Received != 0 {
		t.Fatalf("err=%v stats=%+v", err, ingress.Stats())
	}
}

func TestGRPCSimulatorRejection(t *testing.T) {
	ingress := ingestion.NewServer(8, slog.New(slog.NewTextHandler(io.Discard, nil)))
	target := startGRPC(t, ingestion.NewGRPCServer(ingress))
	cfg := simulator.DefaultConfig()
	cfg.Target = target
	cfg.Count = 1
	result, err := simulator.Run(cfg)
	if err != nil || result.Failed != 1 || result.Received != 0 || result.ErrorTypes["ResourceExhausted"] != 1 || result.AckSamples != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
