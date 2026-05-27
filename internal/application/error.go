package application

import "errors"

var (
	ErrNilRepository = errors.New("search repository is nil")
	ErrNilBroker     = errors.New("event broker is nil")
	ErrNilLogger     = errors.New("logger is nil")
)
