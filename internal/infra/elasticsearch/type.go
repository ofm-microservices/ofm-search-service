package elasticsearch

import (
	"context"
	"search-service/internal/domain"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

// Logger aliases the shared logger contract used by the Elasticsearch adapter.
type Logger = logging.Logger

// Repository stores and queries gig search documents in Elasticsearch.
type Repository interface {
	domain.SearchRepository
	Close() error
}

// Config is the concrete Elasticsearch connection configuration.
type Config struct {
	URL      string
	Index    string
	Username string
	Password string
}

// SearchDocument aliases the search domain document stored in Elasticsearch.
type SearchDocument = domain.GigDocument

// SearchQuery aliases the search domain query.
type SearchQuery = domain.SearchQuery

// SearchPage aliases the search domain page result.
type SearchPage = domain.SearchPage

// SearchResult aliases the search domain result row.
type SearchResult = domain.SearchResult

// DocumentStore publishes the concrete persistence boundary.
type DocumentStore interface {
	UpsertGig(ctx context.Context, doc domain.GigDocument) error
	UpdateGigPicture(ctx context.Context, gigID, picture string) error
	DeleteGig(ctx context.Context, gigID string) error
	Search(ctx context.Context, q domain.SearchQuery) (*domain.SearchPage, error)
}
