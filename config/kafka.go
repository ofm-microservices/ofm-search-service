package config

// KafkaConfig defines the canonical event topic consumed by search-service.
type KafkaConfig struct {
	Brokers         []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"127.0.0.1:9092"`
	GigEventsTopic  string   `env:"KAFKA_GIG_EVENTS_TOPIC" envDefault:"migration.gig-service.gigs.changed"`
	GroupID         string   `env:"KAFKA_SEARCH_GROUP_ID" envDefault:"search-service"`
	DeadLetterTopic string   `env:"KAFKA_SEARCH_DLQ_TOPIC" envDefault:"migration.dead-letter"`
}
