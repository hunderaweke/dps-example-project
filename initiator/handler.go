package initiator

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/internal/router"
)

// newHTTPHandler builds the gin engine, global middleware and Huma API, and
// registers every route.
func newHTTPHandler(cfg *config.Config, p *Platform, mods Modules, logger *zap.Logger) http.Handler {
	if !isDevelopment(cfg) {
		gin.SetMode(gin.ReleaseMode)
	}
	engine := gin.New()
	engine.Use(
		gin.Recovery(),
		otelgin.Middleware(cfg.App.Name),
		router.RequestLogger(logger.Named("http")),
		cors.New(cors.Config{
			AllowOrigins:  cfg.Server.CORSOrigins,
			AllowMethods:  []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
			AllowHeaders:  []string{"Origin", "Content-Type", "Authorization", "traceparent"},
			ExposeHeaders: []string{"Content-Length"},
		}),
	)

	api := newHumaAPI(engine, cfg)
	router.RegisterHealth(api, healthChecks(p))
	registerRoutes(api, mods, logger)
	return engine
}

func newHumaAPI(engine *gin.Engine, cfg *config.Config) huma.API {
	hc := huma.DefaultConfig(cfg.App.Name, cfg.App.Version)
	hc.Info.Description = "DPS audit service: the append-only trail of every DPS domain event."
	return humagin.New(engine, hc)
}

// registerRoutes is the single list of business routes. Add new modules here.
// The audit read API (dps.audit.v1.AuditService) is not exposed yet.
func registerRoutes(_ huma.API, _ Modules, _ *zap.Logger) {}

func healthChecks(p *Platform) map[string]router.Check {
	checks := map[string]router.Check{}
	if p.Postgres != nil {
		checks["postgres"] = func(ctx context.Context) error { return p.Postgres.Ping(ctx) }
	}
	if p.Valkey != nil {
		checks["valkey"] = func(ctx context.Context) error { return p.Valkey.Ping(ctx).Err() }
	}
	if p.Producer != nil {
		checks["kafka"] = func(ctx context.Context) error { return p.Producer.Ping(ctx) }
	}
	return checks
}

// OpenAPI renders the OpenAPI document without connecting to any dependency.
// Used by `make openapi`.
func OpenAPI() ([]byte, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	gin.SetMode(gin.ReleaseMode) // debug route logs go to stdout and would corrupt the spec
	api := newHumaAPI(gin.New(), cfg)
	router.RegisterHealth(api, nil)
	registerRoutes(api, Modules{}, zap.NewNop())
	return api.OpenAPI().YAML()
}
