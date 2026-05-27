package config

// ElasticsearchConfig defines the ES index connection used by search-service.
type ElasticsearchConfig struct {
	URL            string `env:"ELASTICSEARCH_URL,required"`
	Index          string `env:"ELASTICSEARCH_INDEX" envDefault:"gigs"`
	Username       string `env:"ELASTICSEARCH_USERNAME"`
	Password       string `env:"ELASTICSEARCH_PASSWORD"`
	MigrationsPath string `env:"ELASTICSEARCH_MIGRATIONS_PATH" envDefault:"file://migration/elasticsearch/000001_create_gigs.up.json"`
}
