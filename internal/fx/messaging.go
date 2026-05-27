package appfx

import (
	"context"
	"search-service/config"
	app "search-service/internal/application"
	eb "search-service/internal/presentation/event_broker"
	natsbroker "search-service/internal/presentation/event_broker/nats"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// MessagingModule wires NATS broker runtime into search-service.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBroker),
	fx.Invoke(InvokeSubscribeGigPublished),
)

// ProvideEventBroker constructs the concrete NATS broker.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (eb.EventBroker, error) {
	broker, err := natsbroker.NewBroker(cfg.NATS, lg)
	if err != nil {
		lg.Error("connect nats failed", logging.Err(err))
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return broker.Close()
		},
	})

	return broker, nil
}

// InvokeSubscribeGigPublished starts the gig publish subscriber.
func InvokeSubscribeGigPublished(lc fx.Lifecycle, cfg *config.Config, broker eb.EventBroker, svc app.SearchService, lg logging.Logger) error {
	subscriber, err := natsbroker.NewGigPublishedSubscriber(cfg.NATS, broker, svc, lg)
	if err != nil {
		return err
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return subscriber.Subscribe(ctx)
		},
	})

	return nil
}
