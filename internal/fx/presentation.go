package appfx

import (
	"context"
	"search-service/config"
	httpserver "search-service/internal/presentation/grpc"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// PresentationModule wires the gRPC server into the FX graph.
var PresentationModule = fx.Options(
	fx.Provide(ProvideServer),
	fx.Invoke(InvokeStartServer),
)

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
