package module_test

import (
	"context"
	"testing"
	"time"

	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
	"github.com/hunderaweke/dps-audit-service/internal/testutil/perf"
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

// recordCase measures Audit.Record (validation) for one batch size.
type recordCase struct {
	name   string
	batch  int
	budget perf.Budget
}

// Budgets: Apple M1 Pro, 2026-10-07, from the benchstat median of 6 runs:
// 2x ns/op, half the rate, allocs/op +10%. See the add-benchmark skill.
var recordCases = []recordCase{
	{name: "batch_1", batch: 1, budget: perf.Budget{MaxNsPerOp: time.Microsecond, MaxAllocsPerOp: 2}},
	{name: "batch_100", batch: 100, budget: perf.Budget{MaxNsPerOp: 100 * time.Microsecond, MaxAllocsPerOp: 110}},
	{name: "batch_500", batch: 500, budget: perf.Budget{MaxNsPerOp: 750 * time.Microsecond, MaxAllocsPerOp: 550}},
}

func (c recordCase) run(b *testing.B) {
	ctx := context.Background()
	sut := module.NewAudit(module.AuditDeps{Log: nopLog{}})
	batch := make([]models.AuditRecord, c.batch)
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

func BenchmarkRecord(b *testing.B) {
	for _, c := range recordCases {
		b.Run(c.name, c.run)
	}
}

func TestRecordBudget(t *testing.T) {
	for _, c := range recordCases {
		t.Run(c.name, func(t *testing.T) { perf.Enforce(t, "Record/"+c.name, c.run, c.budget) })
	}
}
