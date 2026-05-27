package appfx

import (
	"context"
	"search-service/config"
	"search-service/internal/domain"
	es "search-service/internal/infra/elasticsearch"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// StorageModule wires the Elasticsearch adapter into the FX graph.
var StorageModule = fx.Options(
	fx.Invoke(InvokeRunMigrations),
	fx.Provide(ProvideRepository),
)

// ProvideRepository opens the Elasticsearch repository used by search-service.
func ProvideRepository(cfg *config.Config, lg logging.Logger) (domain.SearchRepository, error) {
	repo, err := es.NewRepository(es.Config{
		URL:      cfg.Elasticsearch.URL,
		Index:    cfg.Elasticsearch.Index,
		Username: cfg.Elasticsearch.Username,
		Password: cfg.Elasticsearch.Password,
	})
	if err != nil {
		lg.Error("open elasticsearch failed", logging.Err(err))
		return nil, err
	}
	lg.Info("elasticsearch connected")
	return repo, nil
}

// InvokeRunMigrations bootstraps the Elasticsearch index used by search-service.
func InvokeRunMigrations(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) error {
	b, err := es.NewBootstrapper(es.Config{
		URL:      cfg.Elasticsearch.URL,
		Index:    cfg.Elasticsearch.Index,
		Username: cfg.Elasticsearch.Username,
		Password: cfg.Elasticsearch.Password,
	})
	if err != nil {
		lg.Error("open elasticsearch bootstrapper failed", logging.Err(err))
		return err
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := b.RunMigrations(ctx, cfg.Elasticsearch.MigrationsPath); err != nil {
				lg.Error("run elasticsearch migration failed", logging.Err(err))
				return err
			}
			lg.Info("elasticsearch migration applied")
			return nil
		},
	})

	return nil
}
