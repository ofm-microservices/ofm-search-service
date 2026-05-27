package application

import (
	"context"
	"search-service/internal/domain"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

type searchService struct {
	repo   domain.SearchRepository
	broker EventBroker
	log    Logger
}

// New constructs the application service responsible for search indexing and queries.
func New(repo domain.SearchRepository, broker EventBroker, log Logger) (SearchService, error) {
	if repo == nil {
		return nil, ErrNilRepository
	}
	if broker == nil {
		return nil, ErrNilBroker
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &searchService{
		repo:   repo,
		broker: broker,
		log:    log.With(logging.String("module", "application")),
	}, nil
}

func (s *searchService) ApplyGigPublished(ctx context.Context, payload []byte) error {
	event, err := parseGigPublished(payload)
	if err != nil {
		return err
	}
	doc := buildDocument(event)
	if strings.TrimSpace(doc.ID) == "" {
		return domain.ErrInvalidQuery
	}

	if err := s.repo.UpsertGig(ctx, doc); err != nil {
		return err
	}

	encoded, err := jsonMarshal(doc)
	if err != nil {
		return err
	}
	return s.broker.Publish(ctx, gigIndexedSubject, encoded)
}

func (s *searchService) ApplyGigDeleted(ctx context.Context, payload []byte) error {
	gigID := strings.TrimSpace(string(payload))
	if gigID == "" {
		return nil
	}
	return s.repo.DeleteGig(ctx, gigID)
}

func (s *searchService) Search(ctx context.Context, q domain.SearchQuery) (*domain.SearchPage, error) {
	if q.Limit <= 0 {
		q.Limit = 20
	}
	return s.repo.Search(ctx, q)
}
