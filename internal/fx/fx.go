package appfx

import "go.uber.org/fx"

// Module is the compatibility bundle of all FX modules used by search-service.
var Module = fx.Options(
	ConfigModule,
	LoggerModule,
	AppModule,
	StorageModule,
	MessagingModule,
	ServiceModule,
	PresentationModule,
)
