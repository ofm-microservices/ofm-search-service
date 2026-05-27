package config

// NATSConfig defines NATS connection and subjects used by search-service.
type NATSConfig struct {
	URL                 string `env:"NATS_URL,required"`
	User                string `env:"NATS_USER"`
	Password            string `env:"NATS_PASSWORD"`
	GigPublishedSubject string `env:"NATS_SUBJECT_GIG_PUBLISHED" envDefault:"gig.published"`
	GigDeletedSubject   string `env:"NATS_SUBJECT_GIG_DELETED" envDefault:"gig.deleted"`
	SearchIndexedSubject string `env:"NATS_SUBJECT_SEARCH_GIG_INDEXED" envDefault:"search.gig.indexed"`
}
