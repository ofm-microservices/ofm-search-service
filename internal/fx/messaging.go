package appfx

import (
	"context"
	"search-service/config"
	app "search-service/internal/application"
	eb "search-service/internal/presentation/event_broker"
	kafkabroker "search-service/internal/presentation/event_broker/kafka"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// MessagingModule wires Kafka canonical events into search-service.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBroker),
	fx.Invoke(InvokeSubscribeGigPublished),
)

// ProvideEventBroker constructs the concrete Kafka broker.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (eb.EventBroker, error) {
	broker, err := kafkabroker.NewBroker(cfg.Kafka, lg)
	if err != nil {
		lg.Error("connect kafka failed", logging.Err(err))
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
	subscriber, err := kafkabroker.NewGigPublishedSubscriber(cfg.Kafka, broker, svc, lg)
	if err != nil {
		return err
	}

	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			go func() {
				if subscribeErr := subscriber.Subscribe(runCtx); subscribeErr != nil && runCtx.Err() == nil {
					lg.Error("search Kafka consumer stopped", logging.Err(subscribeErr))
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})

	return nil
}
