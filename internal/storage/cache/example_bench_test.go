package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"

	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/storage/cache"
)

// Adapter benchmarks run against a real Valkey in Docker; skipped with -short.
func BenchmarkExampleCache(b *testing.B) {
	if testing.Short() {
		b.Skip("requires docker")
	}
	ctx := context.Background()
	ctr, err := tcvalkey.Run(ctx, "valkey/valkey:8-alpine")
	testcontainers.CleanupContainer(b, ctr)
	if err != nil {
		b.Fatal(err)
	}
	url, err := ctr.ConnectionString(ctx)
	if err != nil {
		b.Fatal(err)
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		b.Fatal(err)
	}
	client := redis.NewClient(opts)
	b.Cleanup(func() { _ = client.Close() })

	c := cache.NewExample(client, time.Minute)
	ex := models.Example{ID: uuid.New(), Name: "bench", OwnerID: "acc_1", Status: models.ExampleStatusPending}

	b.Run("set", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := c.Set(ctx, ex); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, _, err := c.Get(ctx, ex.ID); err != nil {
				b.Fatal(err)
			}
		}
	})
}
