package repository

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	apperrors "github.com/username/example-service/internal/const/errors"
	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/module"
)

const auditCollection = "audit_log"

type audit struct {
	coll *mongo.Collection
}

var _ module.AuditStore = (*audit)(nil)

func NewAudit(db *mongo.Database) module.AuditStore {
	return &audit{coll: db.Collection(auditCollection)}
}

func (a *audit) Record(ctx context.Context, entry models.AuditEntry) error {
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now().UTC()
	}
	if _, err := a.coll.InsertOne(ctx, entry); err != nil {
		return apperrors.ErrDBWrite.Wrap(err, "record audit entry")
	}
	return nil
}
