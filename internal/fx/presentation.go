package appfx

import (
	"context"
	"search-service/config"
	httpserver "search-service/internal/presentation/grpc"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	sharedmetrics "github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"go.uber.org/fx"
)

// PresentationModule wires the gRPC server into the FX graph.
var PresentationModule = fx.Options(
	fx.Provide(ProvideServer),
	fx.Invoke(InvokeStartServer),
	fx.Provide(ProvideMeter),
	fx.Invoke(InvokeStartMetrics),
)

// ProvideMeter constructs the service-owned Prometheus meter.
func ProvideMeter(cfg *config.Config) sharedmetrics.Meter {
	meter := sharedmetrics.New(cfg.App.Name, cfg.App.Env)
	sharedmetrics.SetGlobal(meter)
	return meter
}

// InvokeStartMetrics exposes the service-owned Prometheus registry.
func InvokeStartMetrics(lc fx.Lifecycle, cfg *config.Config, meter sharedmetrics.Meter, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{OnStart: func(context.Context) error {
		runCtx, runCancel := context.WithCancel(context.Background())
		cancel = runCancel
		go func() {
			_ = sharedmetrics.StartServer(runCtx, sharedmetrics.Config{Enabled: cfg.Metrics.Enabled, Host: cfg.Metrics.Host, Port: cfg.Metrics.Port, Path: cfg.Metrics.Path}, meter, lg)
		}()
		return nil
	}, OnStop: func(context.Context) error {
		if cancel != nil {
			cancel()
		}
		return nil
	}})
}

// ProvideServer constructs the search gRPC server.
func ProvideServer(svc httpserver.SearchService, cfg *config.Config, lg logging.Logger) (httpserver.Server, error) {
	return httpserver.NewServer(svc, cfg.GRPC, lg)
}

// InvokeStartServer starts and stops the gRPC server with the FX lifecycle.
func InvokeStartServer(lc fx.Lifecycle, srv httpserver.Server) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				_ = srv.Start()
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
}
