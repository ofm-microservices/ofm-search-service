package kafka

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"search-service/config"
	app "search-service/internal/application"
	eventbroker "search-service/internal/presentation/event_broker"
)

// GigPublishedSubscriber consumes canonical gig events and updates search indexes.
type GigPublishedSubscriber struct {
	topic string
	b     eventbroker.EventBroker
	svc   app.SearchService
}

// NewGigPublishedSubscriber constructs the Kafka canonical-event subscriber.
func NewGigPublishedSubscriber(cfg config.KafkaConfig, b eventbroker.EventBroker, svc app.SearchService, _ logging.Logger) (*GigPublishedSubscriber, error) {
	if b == nil {
		return nil, errors.New("event broker is nil")
	}
	if svc == nil {
		return nil, errors.New("search service is nil")
	}
	return &GigPublishedSubscriber{topic: cfg.GigEventsTopic, b: b, svc: svc}, nil
}

// Subscribe starts the canonical gig event consumer.
func (s *GigPublishedSubscriber) Subscribe(ctx context.Context) error {
	return s.b.Subscribe(ctx, s.topic, func(ctx context.Context, _ string, payload []byte) error {
		event, err := mapCanonicalEvent(payload)
		if err != nil {
			return err
		}
		claimed := false
		if event.eventID != "" {
			if claimer, ok := s.svc.(interface {
				ClaimEvent(context.Context, string) (bool, error)
			}); ok {
				claimed, claimErr := claimer.ClaimEvent(ctx, event.eventID)
				if claimErr != nil {
					return claimErr
				}
				if !claimed {
					return nil
				}
			}
		}
		var projectionErr error
		if event.deleted {
			projectionErr = s.svc.ApplyGigDeleted(ctx, []byte(event.aggregateID))
		} else {
			projectionErr = s.svc.ApplyGigPublished(ctx, event.payload)
		}
		if projectionErr != nil && claimed {
			if releaser, ok := s.svc.(interface {
				ReleaseEvent(context.Context, string) error
			}); ok {
				_ = releaser.ReleaseEvent(ctx, event.eventID)
			}
		}
		return projectionErr
	})
}

type canonicalEvent struct {
	eventID     string
	deleted     bool
	aggregateID string
	payload     []byte
}

func mapCanonicalEvent(raw []byte) (canonicalEvent, error) {
	var envelope struct {
		EventID     string          `json:"event_id"`
		Operation   string          `json:"operation"`
		EventType   string          `json:"event_type"`
		AggregateID string          `json:"aggregate_id"`
		Payload     json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return canonicalEvent{}, err
	}
	deleted := envelope.Operation == "d" || envelope.Operation == "delete" || envelope.EventType == "gig.deleted"
	if len(envelope.Payload) == 0 || string(envelope.Payload) == "null" {
		envelope.Payload = raw
	}
	return canonicalEvent{eventID: envelope.EventID, deleted: deleted, aggregateID: envelope.AggregateID, payload: envelope.Payload}, nil
}
