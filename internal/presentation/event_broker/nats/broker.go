package nats

import (
	"context"
	"time"

	"search-service/config"
	eventbroker "search-service/internal/presentation/event_broker"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/natstrace"
)

type broker struct {
	nc  *nats.Conn
	log logging.Logger
}

// NewBroker constructs the concrete NATS broker used by search-service.
func NewBroker(cfg config.NATSConfig, log logging.Logger) (eventbroker.EventBroker, error) {
	if cfg.URL == "" {
		return nil, ErrEmptyNATSURL
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	opts := []nats.Option{
		nats.Name("search-service"),
		nats.MaxReconnects(-1),
	}
	if cfg.User != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, err
	}

	lg := log.With(
		logging.String("module", "nats-broker"),
		logging.String("nats_url", cfg.URL),
	)
	lg.Info("nats broker connected")
	return &broker{nc: nc, log: lg}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	msg := natstrace.NewMessage(ctx, subject, payload)
	if err := b.nc.PublishMsg(msg); err != nil {
		return err
	}
	return flush(ctx, b.nc)
}

func (b *broker) Subscribe(_ context.Context, subject string, handler eventbroker.MessageHandler) error {
	_, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		if err := handler(context.Background(), msg.Subject, msg.Data); err != nil {
			b.log.Error("message handler failed", logging.String("subject", msg.Subject), logging.Err(err))
		}
	})
	if err != nil {
		return err
	}
	return flush(context.Background(), b.nc)
}

func (b *broker) Close() error {
	if b.nc != nil {
		b.log.Info("closing nats broker")
		b.nc.Close()
	}
	return nil
}

func flush(ctx context.Context, nc *nats.Conn) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	return nc.FlushWithContext(ctx)
}
