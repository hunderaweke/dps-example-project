// Package module is the application core. It declares the ports (interfaces)
// it needs and implements the use cases on top of them. It must not import
// transport or storage packages (gin, huma, pgx, go-redis, franz-go, temporal,
// grpc); those are adapters that implement or call these ports and are wired
// together in initiator/.
package module

import (
	"context"

	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"

	apperrors "github.com/hunderaweke/dps-audit-service/internal/const/errors"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
)

// ---- Inbound port (driven by the Kafka handler) ----

type Audit interface {
	// Record stores a batch of audit records. Records already stored (same
	// event_id) are skipped; stored is the number of new rows.
	Record(ctx context.Context, recs []models.AuditRecord) (stored int64, err error)
	Get(ctx context.Context, eventID string) (models.AuditRecord, error)
}

// ---- Outbound port (implemented by storage adapters) ----

// AuditLog is the append-only audit store.
type AuditLog interface {
	Append(ctx context.Context, recs []models.AuditRecord) (int64, error)
	Get(ctx context.Context, eventID string) (models.AuditRecord, error)
}

// ---- Implementation ----

type AuditDeps struct {
	Log    AuditLog
	Logger *zap.Logger
}

type audit struct {
	AuditDeps
	validate *validator.Validate
}

func NewAudit(deps AuditDeps) Audit {
	if deps.Logger == nil {
		deps.Logger = zap.NewNop()
	}
	return &audit{
		AuditDeps: deps,
		validate:  validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (s *audit) Record(ctx context.Context, recs []models.AuditRecord) (int64, error) {
	if len(recs) == 0 {
		return 0, nil
	}
	for i := range recs {
		if err := s.validate.Struct(recs[i]); err != nil {
			return 0, apperrors.ErrInvalidInput.New("audit record %d: %v", i, err)
		}
	}
	stored, err := s.Log.Append(ctx, recs)
	if err != nil {
		return 0, err
	}
	if dups := int64(len(recs)) - stored; dups > 0 {
		s.Logger.Debug("skipped already stored audit records", zap.Int64("duplicates", dups))
	}
	return stored, nil
}

func (s *audit) Get(ctx context.Context, eventID string) (models.AuditRecord, error) {
	if eventID == "" {
		return models.AuditRecord{}, apperrors.ErrInvalidInput.New("event id is required")
	}
	return s.Log.Get(ctx, eventID)
}
