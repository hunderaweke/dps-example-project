package repository_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/internal/const/database/postgres"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository/db"
)

// Adapter benchmarks run against a real Postgres in Docker; skipped with -short.
func BenchmarkExampleRepository(b *testing.B) {
	if testing.Short() {
		b.Skip("requires docker")
	}
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("bench"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(b, ctr)
	if err != nil {
		b.Fatal(err)
	}
	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	if err := postgres.Migrate(url); err != nil {
		b.Fatal(err)
	}
	pool, err := postgres.NewPool(ctx, config.Postgres{URL: url, MaxConns: 10})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(pool.Close)

	repo := repository.NewExample(db.New(pool))
	seed, err := repo.Create(ctx, models.Example{ID: uuid.New(), Name: "seed", OwnerID: "acc_1", Status: models.ExampleStatusPending})
	if err != nil {
		b.Fatal(err)
	}

	b.Run("create", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ex := models.Example{ID: uuid.New(), Name: "bench", OwnerID: "acc_1", Status: models.ExampleStatusPending}
			if _, err := repo.Create(ctx, ex); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := repo.Get(ctx, seed.ID); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("list", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := repo.List(ctx, models.Page{Limit: 20}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
