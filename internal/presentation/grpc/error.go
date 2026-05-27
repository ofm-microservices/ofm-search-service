package grpc

import "errors"

var (
	ErrNilSearchService = errors.New("search service is nil")
	ErrNilLogger       = errors.New("logger is nil")
)
