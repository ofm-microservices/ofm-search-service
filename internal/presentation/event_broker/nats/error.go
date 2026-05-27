package nats

import "errors"

var (
	ErrEmptyNATSURL    = errors.New("nats url is empty")
	ErrNilLogger       = errors.New("logger is nil")
	ErrNilSearchService = errors.New("search service is nil")
	ErrNilEventBroker   = errors.New("event broker is nil")
)
