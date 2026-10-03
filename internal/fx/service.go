package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	app "search-service/internal/application"
	"search-service/internal/domain"
	eb "search-service/internal/presentation/event_broker"
)

// ServiceModule wires the application service into the FX graph.
var ServiceModule = fx.Options(
	fx.Provide(ProvideSearchService),
)

// ProvideSearchService constructs the search application service.
func ProvideSearchService(repo domain.SearchRepository, broker eb.EventBroker, files app.FileURLClient, lg logging.Logger) (app.SearchService, error) {
	return app.New(repo, broker, files, lg)
}
