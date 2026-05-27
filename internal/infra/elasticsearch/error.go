package elasticsearch

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyURL   = errors.New("elasticsearch url is empty")
	ErrNilLogger  = errors.New("logger is nil")
	ErrEmptyIndex = errors.New("elasticsearch index is empty")
	ErrEmptyPath  = errors.New("elasticsearch migrations path is empty")
)

// WrapResolveMigrationPathError annotates relative migration path resolution
// failures.
func WrapResolveMigrationPathError(err error) error {
	return fmt.Errorf("resolve migration path: %w", err)
}

// WrapRunMigrationError annotates Elasticsearch index bootstrap failures.
func WrapRunMigrationError(err error) error {
	return fmt.Errorf("run migration: %w", err)
}
