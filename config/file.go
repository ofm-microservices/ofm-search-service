package config

// FileServiceConfig defines the internal gRPC client address used by
// search-service to resolve file URLs in the background.
type FileServiceConfig struct {
	Address string `env:"FILE_SERVICE_ADDRESS" envDefault:"file-service:9504"`
}
