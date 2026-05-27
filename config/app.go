package config

// AppConfig holds process-level runtime settings for search-service.
type AppConfig struct {
	Name     string `env:"APP_NAME" envDefault:"search-service"`
	Env      string `env:"APP_ENV" envDefault:"local"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
}
