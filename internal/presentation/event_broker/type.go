package event_broker

import "context"

// MessageHandler processes one broker message.
type MessageHandler func(ctx context.Context, subject string, payload []byte) error

// BatchMessageHandler processes a bounded batch fetched from one Kafka partition.
type BatchMessageHandler func(ctx context.Context, subject string, messages []Message) error

// Message contains a Kafka payload and its commit metadata.
type Message struct {
	Key, Value []byte
	Partition  int
	Offset     int64
	Headers    []Header
}

// Header carries Kafka request metadata into a batch handler.
type Header struct {
	Key   string
	Value []byte
}

// EventBroker is the transport-agnostic message boundary for search-service.
type EventBroker interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Subscribe(ctx context.Context, subject string, handler MessageHandler) error
	Close() error
}

// BatchEventBroker is implemented by brokers that support bounded batch consumption.
type BatchEventBroker interface {
	EventBroker
	SubscribeBatch(ctx context.Context, subject string, handler BatchMessageHandler) error
}
