package nats

import (
	"context"
	"encoding/json"

	"search-service/config"
	app "search-service/internal/application"
	eventbroker "search-service/internal/presentation/event_broker"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

// GigPublishedSubscriber consumes gig publish events and indexes them.
type GigPublishedSubscriber struct {
	cfg config.NATSConfig
	b   eventbroker.EventBroker
	svc app.SearchService
	log logging.Logger
}

// NewGigPublishedSubscriber constructs the subscriber.
func NewGigPublishedSubscriber(cfg config.NATSConfig, b eventbroker.EventBroker, svc app.SearchService, log logging.Logger) (*GigPublishedSubscriber, error) {
	if svc == nil || b == nil {
		if svc == nil {
			return nil, ErrNilSearchService
		}
		return nil, ErrNilEventBroker
	}
	return &GigPublishedSubscriber{cfg: cfg, b: b, svc: svc, log: log.With(logging.String("module", "gig-published-subscriber"))}, nil
}

// Subscribe wires the broker subject to the application service.
func (s *GigPublishedSubscriber) Subscribe(ctx context.Context) error {
	if err := s.b.Subscribe(ctx, s.cfg.GigPublishedSubject, func(ctx context.Context, _ string, payload []byte) error {
		return s.svc.ApplyGigPublished(ctx, payload)
	}); err != nil {
		return err
	}
	return s.b.Subscribe(ctx, s.cfg.GigDeletedSubject, func(ctx context.Context, _ string, payload []byte) error {
		return s.svc.ApplyGigDeleted(ctx, payload)
	})
}

// SearchIndexedSubscriber is a placeholder for future fan-out on indexed docs.
type SearchIndexedSubscriber struct{}

// MarshalIndexedPayload keeps the search-index event shape stable for later consumers.
func MarshalIndexedPayload(doc any) ([]byte, error) {
	return json.Marshal(doc)
}
