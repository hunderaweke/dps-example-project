package module_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apperrors "github.com/hunderaweke/dps-audit-service/internal/const/errors"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
	"github.com/hunderaweke/dps-audit-service/internal/module/mocks"
)

func record(i int) models.AuditRecord {
	return models.AuditRecord{
		EventID:       fmt.Sprintf("evt-%d", i),
		EventType:     "ledger.operation.completed.v1",
		AggregateID:   "op-1",
		OccurredAt:    time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC),
		MetadataValid: true,
		Topic:         "ledger.events",
		Offset:        int64(i),
		Payload:       []byte{0, 1, 2},
	}
}

func TestRecord(t *testing.T) {
	ctx := context.Background()

	t.Run("appends the batch and returns the stored count", func(t *testing.T) {
		log := mocks.NewAuditLog(t)
		recs := []models.AuditRecord{record(1), record(2), record(3)}
		log.EXPECT().Append(ctx, recs).Return(2, nil) // one already stored

		stored, err := module.NewAudit(module.AuditDeps{Log: log}).Record(ctx, recs)
		require.NoError(t, err)
		assert.EqualValues(t, 2, stored)
	})

	t.Run("an empty batch does not touch the store", func(t *testing.T) {
		stored, err := module.NewAudit(module.AuditDeps{Log: mocks.NewAuditLog(t)}).Record(ctx, nil)
		require.NoError(t, err)
		assert.Zero(t, stored)
	})

	for name, mutate := range map[string]func(*models.AuditRecord){
		"missing event id":    func(r *models.AuditRecord) { r.EventID = "" },
		"missing event type":  func(r *models.AuditRecord) { r.EventType = "" },
		"missing topic":       func(r *models.AuditRecord) { r.Topic = "" },
		"missing occurred at": func(r *models.AuditRecord) { r.OccurredAt = time.Time{} },
	} {
		t.Run("rejects a record with "+name, func(t *testing.T) {
			bad := record(2)
			mutate(&bad)
			_, err := module.NewAudit(module.AuditDeps{Log: mocks.NewAuditLog(t)}).
				Record(ctx, []models.AuditRecord{record(1), bad})
			require.Error(t, err)
			assert.True(t, errorx.IsOfType(err, apperrors.ErrInvalidInput), "got %v", err)
		})
	}

	t.Run("passes store errors through", func(t *testing.T) {
		log := mocks.NewAuditLog(t)
		log.EXPECT().Append(ctx, []models.AuditRecord{record(1)}).Return(0, apperrors.ErrDBWrite.New("down"))
		_, err := module.NewAudit(module.AuditDeps{Log: log}).Record(ctx, []models.AuditRecord{record(1)})
		assert.True(t, errorx.IsOfType(err, apperrors.ErrDBWrite))
	})
}

func TestGet(t *testing.T) {
	ctx := context.Background()

	t.Run("requires an event id", func(t *testing.T) {
		_, err := module.NewAudit(module.AuditDeps{Log: mocks.NewAuditLog(t)}).Get(ctx, "")
		assert.True(t, errorx.IsOfType(err, apperrors.ErrInvalidInput))
	})

	t.Run("reads from the store", func(t *testing.T) {
		log := mocks.NewAuditLog(t)
		log.EXPECT().Get(ctx, "evt-1").Return(record(1), nil)
		got, err := module.NewAudit(module.AuditDeps{Log: log}).Get(ctx, "evt-1")
		require.NoError(t, err)
		assert.Equal(t, "evt-1", got.EventID)
	})
}
