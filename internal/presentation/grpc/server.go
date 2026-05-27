package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"search-service/config"
	app "search-service/internal/application"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	searchv1 "github.com/ofm-microservices/ofm-common/proto/search/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	searchv1.UnimplementedSearchServiceServer
	svc app.SearchService
	cfg config.GRPCConfig
	log logging.Logger
	srv *grpc.Server
	lis net.Listener
	mapr SearchMapper
}

// NewServer constructs the gRPC server used by search-service.
func NewServer(svc app.SearchService, cfg config.GRPCConfig, log logging.Logger) (Server, error) {
	if svc == nil {
		return nil, ErrNilSearchService
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	grpcSrv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(metrics.UnaryServerInterceptor()),
	)
	s := &server{
		svc:  svc,
		cfg:  cfg,
		log:  log.With(logging.String("module", "grpc-server")),
		srv:  grpcSrv,
		mapr: newSearchMapper(),
	}
	searchv1.RegisterSearchServiceServer(grpcSrv, s)
	return s, nil
}

func (s *server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.lis = lis
	s.log.Info("starting grpc server", logging.String("addr", addr))
	return s.srv.Serve(lis)
}

func (s *server) Shutdown(context.Context) error {
	if s.srv != nil {
		s.log.Info("shutting down grpc server")
		s.srv.GracefulStop()
	}
	if s.lis != nil {
		return s.lis.Close()
	}
	return nil
}

func (s *server) Search(ctx context.Context, req *searchv1.SearchRequest) (*searchv1.SearchResponse, error) {
	started := time.Now()
	res, err := s.svc.Search(ctx, s.mapr.ToQuery(req))
	if err != nil {
		s.log.Error("search failed",
			logging.Operation("grpc.search.search"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.Err(err),
		)
		return nil, status.Error(codes.Internal, "internal server error")
	}
	return s.mapr.ToResponse(&app.SearchResultPage{
		Services: res.Services,
		Cursor:   res.Cursor,
		HasMore:  res.HasMore,
	}), nil
}
