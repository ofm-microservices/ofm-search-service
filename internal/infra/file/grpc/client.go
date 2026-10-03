package grpc

import (
	"context"
	"strings"

	"search-service/config"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	filev1 "github.com/ofm-microservices/ofm-common/proto/file/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type client struct {
	conn *grpcpkg.ClientConn
	cl   filev1.FileServiceClient
	log  logging.Logger
}

// New constructs the file-service gRPC client used by search-service.
func New(cfg config.FileServiceConfig, log logging.Logger) (Client, error) {
	if log == nil {
		return nil, ErrNilLogger
	}
	addr := strings.TrimSpace(cfg.Address)
	if addr == "" {
		return nil, ErrEmptyAddress
	}

	conn, err := grpcpkg.NewClient(
		addr,
		grpcpkg.WithTransportCredentials(insecure.NewCredentials()),
		grpcpkg.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpcpkg.WithUnaryInterceptor(metrics.UnaryClientInterceptor()),
	)
	if err != nil {
		return nil, err
	}

	lg := log.With(logging.String("module", "file-grpc-client"), logging.String("address", addr))
	lg.Info("file gRPC client connected")
	return &client{
		conn: conn,
		cl:   filev1.NewFileServiceClient(conn),
		log:  lg,
	}, nil
}

// GetFileURL resolves a public file URL by file identifier.
func (c *client) GetFileURL(ctx context.Context, fileID string) (string, error) {
	res, err := c.cl.GetFileURL(ctx, &filev1.GetFileURLRequest{FileId: strings.TrimSpace(fileID)})
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return strings.TrimSpace(res.GetUrl()), nil
}

// Close shuts down the underlying gRPC client connection.
func (c *client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
