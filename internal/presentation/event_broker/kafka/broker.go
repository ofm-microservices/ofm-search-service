package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"search-service/config"
	eventbroker "search-service/internal/presentation/event_broker"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	requestmetadata "github.com/ofm-microservices/ofm-common/pkg/observability/metadata"
	sharedmetrics "github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
)

var errEmptyBrokers = errors.New("kafka brokers are empty")

type broker struct {
	brokers    []string
	group      string
	log        logging.Logger
	deadLetter string
	mu         sync.Mutex
	readers    []*kafka.Reader
}

// NewBroker constructs the Kafka adapter for canonical service events.
func NewBroker(cfg config.KafkaConfig, log logging.Logger) (eventbroker.EventBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errEmptyBrokers
	}
	if log == nil {
		return nil, errors.New("logger is nil")
	}
	return &broker{brokers: cfg.Brokers, group: cfg.GroupID, deadLetter: cfg.DeadLetterTopic, log: log}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	if err := b.ensureTopic(ctx, subject); err != nil {
		return err
	}
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, Balancer: &kafka.Hash{}}
	defer w.Close()
	return w.WriteMessages(ctx, kafka.Message{Value: payload, Headers: kafkaHeaders(ctx)})
}

func kafkaHeaders(ctx context.Context) []kafka.Header {
	headers := make([]kafka.Header, 0, 5)
	for key, value := range requestmetadata.OutgoingHeaders(ctx) {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return headers
}

func (b *broker) Subscribe(ctx context.Context, subject string, handler eventbroker.MessageHandler) error {
	if err := b.ensureTopic(ctx, b.deadLetter); err != nil {
		return err
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     b.brokers,
		Topic:       subject,
		GroupID:     b.group,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		Dialer:      &kafka.Dialer{Timeout: 5 * time.Second, DualStack: true},
	})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	go func() {
		defer r.Close()
		for {
			msg, err := r.FetchMessage(ctx)
			if err != nil {
				return
			}
			attempts := 0
			err = resilience.Retry(ctx, resilience.RetryPolicyFromEnv(), func(attemptCtx context.Context, attempt int) error {
				attempts = attempt
				err := handler(kafkaprop.Context(attemptCtx, msg.Headers), subject, msg.Value)
				if err != nil {
					b.log.Error("kafka message handler failed", logging.String("topic", subject), logging.Int("attempt", attempt), logging.Err(err))
				}
				return err
			})
			if err == nil {
				if err := r.CommitMessages(ctx, msg); err != nil {
					return
				}
				continue
			}
			writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, Balancer: &kafka.Hash{}, WriteTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second}
			dlqRecord, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{
				OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: subject,
				OriginalPartition: msg.Partition, OriginalOffset: msg.Offset,
				Attempts: attempts, ErrorClass: fmt.Sprintf("%T", err), Error: err.Error(), FailedAt: time.Now().UTC(),
			})
			if marshalErr != nil {
				b.log.Error("kafka DLQ envelope marshal failed", logging.String("topic", subject), logging.Err(marshalErr))
				continue
			}
			dlq := kafka.Message{Key: msg.Key, Value: dlqRecord, Headers: append(msg.Headers,
				kafka.Header{Key: "original-topic", Value: []byte(subject)},
				kafka.Header{Key: "original-partition", Value: []byte(fmt.Sprint(msg.Partition))},
				kafka.Header{Key: "original-offset", Value: []byte(fmt.Sprint(msg.Offset))},
				kafka.Header{Key: "delivery-count", Value: []byte(fmt.Sprint(attempts))},
				kafka.Header{Key: "dead-letter-reason", Value: []byte(err.Error())},
			)}
			publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			dlqErr := writer.WriteMessages(publishCtx, dlq)
			cancel()
			_ = writer.Close()
			if dlqErr != nil {
				b.log.Error("kafka dead-letter publish failed", logging.String("topic", subject), logging.Err(dlqErr))
				continue
			}
			sharedmetrics.IncKafkaDLQ(b.deadLetter)
			if err := r.CommitMessages(ctx, msg); err != nil {
				return
			}
		}
	}()
	return nil
}

func (b *broker) ensureTopic(ctx context.Context, topic string) error {
	if topic == "" {
		return errors.New("kafka dead-letter topic is empty")
	}
	conn, err := kafka.DialLeader(ctx, "tcp", b.brokers[0], topic, 0)
	if err == nil {
		return conn.Close()
	}
	conn, err = kafka.DialContext(ctx, "tcp", b.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
}

func (b *broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
	return nil
}

// IsDeletedEvent recognizes the canonical tombstone/operation forms emitted by the migration bridge.
func IsDeletedEvent(payload []byte) bool {
	var v struct {
		Operation string `json:"operation"`
		EventType string `json:"event_type"`
	}
	if json.Unmarshal(payload, &v) != nil {
		return false
	}
	return v.Operation == "d" || v.Operation == "delete" || v.EventType == "gig.deleted"
}
