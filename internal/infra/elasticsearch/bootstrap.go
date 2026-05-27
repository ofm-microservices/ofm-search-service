package elasticsearch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type bootstrapper struct {
	cl *client
}

// NewBootstrapper constructs the Elasticsearch index bootstrapper.
func NewBootstrapper(cfg Config) (*bootstrapper, error) {
	cl, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return &bootstrapper{cl: cl}, nil
}

// RunMigrations creates the configured Elasticsearch index from the bootstrap
// file when it does not already exist.
func (b *bootstrapper) RunMigrations(ctx context.Context, path string) error {
	if strings.TrimSpace(path) == "" {
		return ErrEmptyPath
	}
	raw, err := readMigrationFile(path)
	if err != nil {
		return err
	}
	exists, err := b.cl.exists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := b.cl.createIndex(ctx, raw); err != nil {
		return WrapRunMigrationError(err)
	}
	return nil
}

func readMigrationFile(path string) ([]byte, error) {
	if after, ok := strings.CutPrefix(path, "file://"); ok {
		raw := after
		if !filepath.IsAbs(raw) {
			abs, err := filepath.Abs(raw)
			if err != nil {
				return nil, WrapResolveMigrationPathError(err)
			}
			path = abs
		} else {
			path = raw
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read migration file: %w", err)
	}
	return data, nil
}
