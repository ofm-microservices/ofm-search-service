package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config groups the full search-service runtime configuration.
type Config struct {
	App           AppConfig
	GRPC          GRPCConfig
	FileService   FileServiceConfig
	Kafka         KafkaConfig
	Elasticsearch ElasticsearchConfig
	Metrics       MetricsConfig
}

// MetricsConfig controls the Prometheus endpoint owned by search-service.
type MetricsConfig struct {
	Enabled bool   `env:"METRICS_ENABLED" envDefault:"true"`
	Host    string `env:"METRICS_HOST" envDefault:"0.0.0.0"`
	Port    int    `env:"METRICS_PORT" envDefault:"9612"`
	Path    string `env:"METRICS_PATH" envDefault:"/metrics"`
}

// Load reads environment variables into Config and applies defaults.
func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, WrapParseEnvConfigError(err)
	}

	return cfg, nil
}
