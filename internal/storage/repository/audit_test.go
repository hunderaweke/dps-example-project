package repository_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/internal/const/database/postgres"
	apperrors "github.com/hunderaweke/dps-audit-service/internal/const/errors"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository/db"
	"github.com/hunderaweke/dps-audit-service/internal/testutil/perf"
)

// newPool starts Postgres in Docker and applies every migration.
func newPool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("audit"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(tb, ctr)
	require.NoError(tb, err)
	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(tb, err)
	require.NoError(tb, postgres.Migrate(url))
	pool, err := postgres.NewPool(ctx, config.Postgres{URL: url, MaxConns: 10})
	require.NoError(tb, err)
	tb.Cleanup(pool.Close)
	return pool
}

func auditRecord(id string, offset int64) models.AuditRecord {
	return models.AuditRecord{
		EventID:          id,
		EventType:        "ledger.operation.completed.v1",
		AggregateID:      "op-42",
		AggregateVersion: 3,
		Producer:         "ledger",
		ProducerImpl:     "real",
		CorrelationID:    "corr-1",
		CausationID:      "evt-0",
		Actor:            models.Actor{Type: "customer", ID: "cust-7", Role: ""},
		Channel:          "mobile_app",
		OccurredAt:       time.Date(2026, 10, 5, 12, 0, 0, 123456000, time.UTC),
		MetadataValid:    true,
		Topic:            "ledger.events",
		Partition:        4,
		Offset:           offset,
		Payload:          []byte{0x00, 0x00, 0x00, 0x00, 0x07, 0x02, 0x0a, 0x01},
	}
}

func TestAuditRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("requires docker")
	}
	ctx := context.Background()
	pool := newPool(t)
	repo := repository.NewAudit(db.New(pool))

	batch := []models.AuditRecord{auditRecord("evt-1", 10), auditRecord("evt-2", 11), auditRecord("evt-3", 12)}

	t.Run("appends a batch once; a redelivery stores nothing", func(t *testing.T) {
		n, err := repo.Append(ctx, batch)
		require.NoError(t, err)
		assert.EqualValues(t, 3, n)

		n, err = repo.Append(ctx, batch)
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("get returns every field", func(t *testing.T) {
		got, err := repo.Get(ctx, "evt-2")
		require.NoError(t, err)
		want := batch[1]
		assert.False(t, got.RecordedAt.IsZero())
		got.RecordedAt = time.Time{}
		assert.True(t, want.OccurredAt.Equal(got.OccurredAt))
		got.OccurredAt = want.OccurredAt
		assert.Equal(t, want, got)
	})

	t.Run("get of an unknown id is not found", func(t *testing.T) {
		_, err := repo.Get(ctx, "nope")
		assert.True(t, errorx.IsOfType(err, apperrors.ErrNotFound), "got %v", err)
	})

	t.Run("the table refuses UPDATE and DELETE", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE audit_records SET event_type = 'x' WHERE event_id = 'evt-1'`)
		require.ErrorContains(t, err, "append-only")
		_, err = pool.Exec(ctx, `DELETE FROM audit_records WHERE event_id = 'evt-1'`)
		require.ErrorContains(t, err, "append-only")
	})
}

// Adapter benchmarks run against a real Postgres in Docker; skipped with -short.
// One container serves every case of a run.

// benchSeq gives every benchmark row its own event ID across cases and runs.
var benchSeq atomic.Int64

func uniqueBatch(n int) []models.AuditRecord {
	batch := make([]models.AuditRecord, n)
	for i := range batch {
		id := benchSeq.Add(1)
		batch[i] = auditRecord(fmt.Sprintf("bench-%d", id), id)
	}
	return batch
}

func reportRowsPerSec(b *testing.B, rowsPerOp int) {
	b.ReportMetric(float64(rowsPerOp)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
}

// repoCase is one scenario against the shared repository.
type repoCase struct {
	name   string
	run    func(repo module.AuditLog) func(*testing.B)
	budget perf.Budget
}

// appendNew appends batches of fresh rows: the consumer's steady state.
func appendNew(size int) func(module.AuditLog) func(*testing.B) {
	return func(repo module.AuditLog) func(*testing.B) {
		return func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := repo.Append(ctx, uniqueBatch(size)); err != nil {
					b.Fatal(err)
				}
			}
			reportRowsPerSec(b, size)
		}
	}
}

// appendDuplicate re-appends a stored batch: a redelivery after a rebalance,
// which goes through ON CONFLICT (event_id) DO NOTHING.
func appendDuplicate(size int) func(module.AuditLog) func(*testing.B) {
	return func(repo module.AuditLog) func(*testing.B) {
		return func(b *testing.B) {
			ctx := context.Background()
			batch := uniqueBatch(size)
			if _, err := repo.Append(ctx, batch); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				n, err := repo.Append(ctx, batch)
				if err != nil {
					b.Fatal(err)
				}
				if n != 0 {
					b.Fatalf("stored %d duplicates", n)
				}
			}
			reportRowsPerSec(b, size)
		}
	}
}

// appendParallel appends fresh batches from many goroutines, as the consumer
// does with one goroutine per partition, contending for the pool.
func appendParallel(size int) func(module.AuditLog) func(*testing.B) {
	return func(repo module.AuditLog) func(*testing.B) {
		return func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := repo.Append(ctx, uniqueBatch(size)); err != nil {
						b.Error(err)
						return
					}
				}
			})
			reportRowsPerSec(b, size)
		}
	}
}

func get(repo module.AuditLog) func(*testing.B) {
	return func(b *testing.B) {
		ctx := context.Background()
		batch := uniqueBatch(1)
		if _, err := repo.Append(ctx, batch); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := repo.Get(ctx, batch[0].EventID); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// Budgets: Apple M1 Pro, 2026-10-07, from the benchstat median of 6 runs:
// 2x ns/op, half the rate, allocs/op +10%. See the add-benchmark skill.
var repoCases = []repoCase{
	{name: "append/batch_1", run: appendNew(1),
		budget: perf.Budget{MaxNsPerOp: 600 * time.Microsecond, MaxAllocsPerOp: 113, MinPerSec: map[string]float64{"rows/s": 1750}}},
	{name: "append/batch_100", run: appendNew(100),
		budget: perf.Budget{MaxNsPerOp: 4500 * time.Microsecond, MaxAllocsPerOp: 2203, MinPerSec: map[string]float64{"rows/s": 22000}}},
	{name: "append/batch_500", run: appendNew(500),
		budget: perf.Budget{MaxNsPerOp: 18500 * time.Microsecond, MaxAllocsPerOp: 10571, MinPerSec: map[string]float64{"rows/s": 27000}}},
	{name: "append/duplicate_batch_100", run: appendDuplicate(100),
		budget: perf.Budget{MaxNsPerOp: 2800 * time.Microsecond, MaxAllocsPerOp: 1872, MinPerSec: map[string]float64{"rows/s": 36000}}},
	{name: "append_parallel/batch_100", run: appendParallel(100),
		budget: perf.Budget{MaxNsPerOp: 1600 * time.Microsecond, MaxAllocsPerOp: 2203, MinPerSec: map[string]float64{"rows/s": 63000}}},
	{name: "get", run: get,
		budget: perf.Budget{MaxNsPerOp: 420 * time.Microsecond, MaxAllocsPerOp: 32}},
}

func BenchmarkAuditRepository(b *testing.B) {
	if testing.Short() {
		b.Skip("requires docker")
	}
	repo := repository.NewAudit(db.New(newPool(b)))
	for _, c := range repoCases {
		b.Run(c.name, c.run(repo))
	}
}

func TestAuditRepositoryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("requires docker")
	}
	if os.Getenv(perf.EnvVar) != "1" {
		t.Skipf("set %s=1 to enforce performance budgets", perf.EnvVar)
	}
	repo := repository.NewAudit(db.New(newPool(t)))
	for _, c := range repoCases {
		t.Run(c.name, func(t *testing.T) { perf.Enforce(t, "AuditRepository/"+c.name, c.run(repo), c.budget) })
	}
}
