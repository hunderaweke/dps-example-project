package initiator

import (
	"go.uber.org/zap"

	"github.com/username/example-service/config"
	"github.com/username/example-service/internal/handler/workflow"
	"github.com/username/example-service/internal/module"
	accountadapter "github.com/username/example-service/internal/storage/account"
	"github.com/username/example-service/internal/storage/cache"
	"github.com/username/example-service/internal/storage/publisher"
	"github.com/username/example-service/internal/storage/repository"
	"github.com/username/example-service/internal/storage/repository/db"
	"github.com/username/example-service/pkg/account"
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
	if p.AccountConn != nil {
		deps.Accounts = accountadapter.New(account.NewClient(p.AccountConn, cfg.Account.Timeout))
	}
	if p.MongoDB != nil {
		deps.Audit = repository.NewAudit(p.MongoDB)
	}

	return Modules{Example: module.NewExample(deps)}
}
