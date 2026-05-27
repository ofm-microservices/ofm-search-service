package grpc

import (
	"context"
	"search-service/internal/application"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	searchv1 "github.com/ofm-microservices/ofm-common/proto/search/v1"
)

// Logger aliases the shared logger contract used by the gRPC adapter.
type Logger = logging.Logger

// SearchService aliases the application boundary.
type SearchService = application.SearchService

// Server defines the gRPC server lifecycle exposed by search-service.
type Server interface {
	Start() error
	Shutdown(ctx context.Context) error
}

// SearchMapper translates between search-service and the public proto contract.
type SearchMapper interface {
	ToQuery(req *searchv1.SearchRequest) application.SearchQuery
	ToResponse(res *application.SearchResultPage) *searchv1.SearchResponse
}
