package module_test

import (
	"context"
	"testing"

	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
)

// nopLog is a hand-written fake so the benchmark measures the use case, not
// mock bookkeeping.
type nopLog struct{}

func (nopLog) Append(_ context.Context, recs []models.AuditRecord) (int64, error) {
	return int64(len(recs)), nil
}

func (nopLog) Get(context.Context, string) (models.AuditRecord, error) {
	return models.AuditRecord{}, nil
}

func BenchmarkRecord(b *testing.B) {
	ctx := context.Background()
	sut := module.NewAudit(module.AuditDeps{Log: nopLog{}})
	batch := make([]models.AuditRecord, 100)
	for i := range batch {
		batch[i] = record(i)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := sut.Record(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}
