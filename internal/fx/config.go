package appfx

import (
	"search-service/config"

	"go.uber.org/fx"
)

// ConfigModule provides configuration loading for search-service.
var ConfigModule = fx.Options(
	fx.Provide(ProvideConfig),
)

// ProvideConfig loads and validates the search-service configuration.
func ProvideConfig() (*config.Config, error) {
	return config.Load()
}
