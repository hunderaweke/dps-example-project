package initiator

import (
	"go.uber.org/zap"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/internal/module"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository/db"
)

type Modules struct {
	Audit module.Audit
}

// newModules plugs outbound adapters into the core. Adapters whose platform
// client was not created for this process are left nil.
func newModules(_ *config.Config, p *Platform, logger *zap.Logger) Modules {
	deps := module.AuditDeps{Logger: logger.Named("audit")}
	if p.Postgres != nil {
		deps.Log = repository.NewAudit(db.New(p.Postgres))
	}
	return Modules{Audit: module.NewAudit(deps)}
}
