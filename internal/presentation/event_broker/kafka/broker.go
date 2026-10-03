package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	commonevents "github.com/ofm-microservices/ofm-common/pkg/events"
	transportkafka "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
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
	enveloped, _, err := commonevents.Wrap(subject, payload)
	if err != nil {
		return err
	}
	if err := b.ensureTopic(ctx, subject); err != nil {
		return err
	}
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, Balancer: &kafka.Hash{}, BatchSize: 100, BatchTimeout: 50 * time.Millisecond}
	defer w.Close()
	transportkafka.Published(subject, enveloped)
	return w.WriteMessages(ctx, kafka.Message{Value: enveloped, Headers: kafkaHeaders(ctx)})
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
	go func() {
		_ = (resilience.KafkaRetryQueueConfig{Brokers: b.brokers, Group: b.group, MaxAttempts: resilience.DefaultRetryPolicy.MaxAttempts}).Run(ctx)
	}()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     b.brokers,
		Topic:       subject,
		GroupID:     b.group,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     50 * time.Millisecond,
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
			attempts := retryAttempt(msg.Headers)
			payload, _, unwrapErr := commonevents.Unwrap(msg.Value)
			if unwrapErr != nil {
				_ = b.deadLetterMessage(ctx, subject, msg, attempts, unwrapErr)
				continue
			}
			transportkafka.Consumed(subject, msg.Partition, msg.Offset, attempts, payload)
			err = handler(kafkaprop.Context(ctx, msg.Headers), subject, payload)
			if err == nil {
				if err := r.CommitMessages(ctx, msg); err != nil {
					return
				}
				continue
			}
			var permanent resilience.PermanentError
			if !errors.As(err, &permanent) && attempts < resilience.DefaultRetryPolicy.MaxAttempts {
				writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: resilience.RetryTopic(b.group), WriteTimeout: 5 * time.Second}
				queueErr := (resilience.KafkaRetryQueue{Writer: writer}).Enqueue(ctx, msg, subject, attempts+1, err)
				_ = writer.Close()
				if queueErr != nil {
					b.log.Error("kafka retry publish failed", logging.Err(queueErr))
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

func retryAttempt(headers []kafka.Header) int {
	for _, h := range headers {
		if h.Key == "x-ofm-retry-attempt" {
			if n, err := strconv.Atoi(string(h.Value)); err == nil && n > 0 {
				return n
			}
		}
	}
	return 1
}

func (b *broker) deadLetterMessage(ctx context.Context, subject string, msg kafka.Message, attempts int, cause error) error {
	payload, err := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", cause), Error: cause.Error(), FailedAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, WriteTimeout: 5 * time.Second}
	err = writer.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: payload})
	_ = writer.Close()
	if err != nil {
		return err
	}
	sharedmetrics.IncKafkaDLQ(b.deadLetter)
	return nil
}

// SubscribeBatch consumes up to 100 messages or 200ms and commits every
// message in the successful batch. Passing the complete batch to kafka-go is
// required because a reader may return messages from multiple partitions;
// committing only the last message leaves the other partitions lagging.
func (b *broker) SubscribeBatch(ctx context.Context, subject string, handler eventbroker.BatchMessageHandler) error {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: subject, GroupID: b.group, StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 10e6, MaxWait: 50 * time.Millisecond, Dialer: &kafka.Dialer{Timeout: 5 * time.Second, DualStack: true}})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	go func() {
		defer r.Close()
		for {
			messages := make([]eventbroker.Message, 0, 100)
			first, err := r.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				b.log.Error("kafka batch fetch failed", logging.String("topic", subject), logging.Err(err))
				time.Sleep(250 * time.Millisecond)
				continue
			}
			messages = append(messages, toBatchMessage(first))
			deadline := time.NewTimer(200 * time.Millisecond)
			for len(messages) < 100 {
				select {
				case <-deadline.C:
					goto process
				default:
				}
				fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
				msg, fetchErr := r.FetchMessage(fetchCtx)
				cancel()
				if fetchErr != nil {
					break
				}
				messages = append(messages, toBatchMessage(msg))
			}
		process:
			if !deadline.Stop() {
				select {
				case <-deadline.C:
				default:
				}
			}
			if err := resilience.Retry(ctx, resilience.RetryPolicy{MaxAttempts: 1}, func(attemptCtx context.Context, _ int) error {
				return handler(attemptCtx, subject, messages)
			}); err != nil {
				b.log.Error("kafka batch handler failed", logging.String("topic", subject), logging.Err(err))
				continue
			}
			commitMessages := make([]kafka.Message, 0, len(messages))
			for _, message := range messages {
				commitMessages = append(commitMessages, kafka.Message{
					Topic: subject, Partition: message.Partition, Offset: message.Offset,
				})
			}
			if err := r.CommitMessages(ctx, commitMessages...); err != nil {
				b.log.Error("kafka batch commit failed", logging.String("topic", subject), logging.Int("messages", len(commitMessages)), logging.Err(err))
				if ctx.Err() != nil {
					return
				}
			}
		}
	}()
	return nil
}

func toBatchMessage(msg kafka.Message) eventbroker.Message {
	payload, _, _ := commonevents.Unwrap(msg.Value)
	headers := make([]eventbroker.Header, 0, len(msg.Headers))
	for _, h := range msg.Headers {
		headers = append(headers, eventbroker.Header{Key: h.Key, Value: h.Value})
	}
	return eventbroker.Message{Key: msg.Key, Value: payload, Partition: msg.Partition, Offset: msg.Offset, Headers: headers}
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
