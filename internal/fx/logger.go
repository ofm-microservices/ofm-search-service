package appfx

import (
	"search-service/config"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// LoggerModule provides the service logger.
var LoggerModule = fx.Options(
	fx.Provide(ProvideLogger),
)

// ProvideLogger builds the search-service logger from runtime configuration.
func ProvideLogger(cfg *config.Config) (logging.Logger, error) {
	return logging.New(cfg.App.Name, cfg.App.Env, cfg.App.LogLevel)
}
