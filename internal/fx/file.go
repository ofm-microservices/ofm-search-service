package appfx

import (
	"context"
	"search-service/config"
	app "search-service/internal/application"
	filegrpc "search-service/internal/infra/file/grpc"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// FileModule wires the file-service gRPC client into the FX graph.
var FileModule = fx.Options(
	fx.Provide(ProvideFileURLClient),
)

// ProvideFileURLClient constructs the file-service gRPC client used for
// search-side picture enrichment.
func ProvideFileURLClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.FileURLClient, error) {
	client, err := filegrpc.New(cfg.FileService, lg)
	if err != nil {
		lg.Error("connect file service failed", logging.Err(err))
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return client.Close()
		},
	})

	return client, nil
}
