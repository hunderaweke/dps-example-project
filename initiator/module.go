package initiator

import (
	"go.uber.org/zap"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/internal/handler/workflow"
	"github.com/hunderaweke/dps-audit-service/internal/module"
	"github.com/hunderaweke/dps-audit-service/internal/storage/cache"
	"github.com/hunderaweke/dps-audit-service/internal/storage/publisher"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository/db"
)

type Modules struct {
	Example module.Example
}

// newModules plugs outbound adapters into the core. Adapters whose platform
// client was not created for this process are left nil; the API never calls
// the worker-only use cases (VerifyOwner, MarkProcessed, HandleCreated).
func newModules(cfg *config.Config, p *Platform, logger *zap.Logger) Modules {
	deps := module.ExampleDeps{Logger: logger.Named("example")}

	if p.Postgres != nil {
		deps.Repo = repository.NewExample(db.New(p.Postgres))
	}
	if p.Valkey != nil {
		deps.Cache = cache.NewExample(p.Valkey, cfg.Valkey.TTL)
	}
	if p.Producer != nil {
		deps.Publisher = publisher.NewExample(p.Producer, cfg.Kafka.Topics.ExampleCreated)
	}
	if p.Temporal != nil {
		deps.Workflows = workflow.NewStarter(p.Temporal, cfg.Temporal.TaskQueue)
	}
	if p.MongoDB != nil {
		deps.Audit = repository.NewAudit(p.MongoDB)
	}

	return Modules{Example: module.NewExample(deps)}
}
