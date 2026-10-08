package ingestion

import (
	"context"
	"errors"

	pb "github.com/jaiswaladi246/streampulse/proto/streampulsev1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type grpcService struct {
	pb.UnimplementedEventServiceServer
	ingress *Server
}

func NewGRPCServer(ingress *Server) *grpc.Server {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(ingress.maxPayload + 4096))
	pb.RegisterEventServiceServer(server, &grpcService{ingress: ingress})
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("streampulse.v1.EventService", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	reflection.Register(server)
	return server
}

func (s *grpcService) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	var err error
	var event Event
	if request == nil || request.Event == nil {
		err = errors.New("event is required")
	} else {
		input := request.Event
		event = Event{EventID: input.EventId, EventType: input.EventType, Source: input.Source, OccurredAtMS: input.OccurredAtMs, SchemaVersion: input.SchemaVersion, Payload: input.PayloadJson, UserID: input.UserId}
		err = Validate(event, s.ingress.maxPayload)
	}
	if err != nil {
		s.ingress.rejected.Add(1)
		code := codes.InvalidArgument
		if len(event.Payload) > s.ingress.maxPayload {
			code = codes.ResourceExhausted
		}
		return nil, status.Error(code, err.Error())
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	s.ingress.record(event)
	return &pb.PublishResponse{EventId: event.EventID, Received: true, KafkaAccepted: false}, nil
}
