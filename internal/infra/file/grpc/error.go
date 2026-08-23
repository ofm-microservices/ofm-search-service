package grpc

import "errors"

var (
	// ErrEmptyAddress reports that the file-service gRPC address is missing.
	ErrEmptyAddress = errors.New("file service address is empty")
	// ErrNilLogger reports a missing logger dependency.
	ErrNilLogger = errors.New("logger is nil")
)
