package application

import (
	"context"
	"search-service/internal/domain"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

// Logger aliases the shared structured logger used by the application layer.
type Logger = logging.Logger

// EventBroker publishes search-side events.
type EventBroker interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Close() error
}

// SearchService owns gig indexing and public search queries.
type SearchService interface {
	ApplyGigPublished(ctx context.Context, payload []byte) error
	ApplyGigDeleted(ctx context.Context, payload []byte) error
	Search(ctx context.Context, q domain.SearchQuery) (*domain.SearchPage, error)
}

// SearchResult aliases the public result shape.
type SearchResult = domain.SearchResult

// SearchQuery aliases the public search query.
type SearchQuery = domain.SearchQuery

// SearchResultPage aliases a page of search results.
type SearchResultPage = domain.SearchPage
