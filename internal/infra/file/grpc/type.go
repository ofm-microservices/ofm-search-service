package grpc

import (
	app "search-service/internal/application"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

// Logger aliases the shared structured logger used by the file gRPC client.
type Logger = logging.Logger

// Client resolves file URLs from file-service for background search enrichment.
type Client interface {
	app.FileURLClient
	Close() error
}
