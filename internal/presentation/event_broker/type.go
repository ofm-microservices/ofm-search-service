package event_broker

import "context"

// MessageHandler processes one broker message.
type MessageHandler func(ctx context.Context, subject string, payload []byte) error

// EventBroker is the transport-agnostic message boundary for search-service.
type EventBroker interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Subscribe(ctx context.Context, subject string, handler MessageHandler) error
	Close() error
}
