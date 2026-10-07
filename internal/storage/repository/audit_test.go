package repository_test

import (
	"context"
	"fmt"
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
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository/db"
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
func BenchmarkAuditRepository(b *testing.B) {
	if testing.Short() {
		b.Skip("requires docker")
	}
	ctx := context.Background()
	repo := repository.NewAudit(db.New(newPool(b)))

	var n int
	b.Run("append batch of 100", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			batch := make([]models.AuditRecord, 100)
			for i := range batch {
				n++
				batch[i] = auditRecord(fmt.Sprintf("bench-%d", n), int64(n))
			}
			if _, err := repo.Append(ctx, batch); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := repo.Get(ctx, "bench-1"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
