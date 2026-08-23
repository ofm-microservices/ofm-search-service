package application

import "errors"

var (
	ErrNilRepository = errors.New("search repository is nil")
	ErrNilBroker     = errors.New("event broker is nil")
	// ErrNilFileClient reports a missing file URL client dependency.
	ErrNilFileClient = errors.New("file client is nil")
	ErrNilLogger     = errors.New("logger is nil")
)
