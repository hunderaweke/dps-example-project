package module_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/module"
)

// In-memory fakes keep the benchmark focused on business logic (validation,
// ID generation, cache-aside flow) rather than mock framework overhead.
type memRepo struct {
	module.ExampleRepository
	mu   sync.RWMutex
	rows map[uuid.UUID]models.Example
}

func (r *memRepo) Create(_ context.Context, ex models.Example) (models.Example, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[ex.ID] = ex
	return ex, nil
}

func (r *memRepo) Get(_ context.Context, id uuid.UUID) (models.Example, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.rows[id], nil
}

type memCache struct {
	module.ExampleCache
	hit bool
	ex  models.Example
}

func (c *memCache) Get(context.Context, uuid.UUID) (models.Example, bool, error) {
	return c.ex, c.hit, nil
}
func (c *memCache) Set(context.Context, models.Example) error { return nil }

type nopPublisher struct{}

func (nopPublisher) PublishExampleCreated(context.Context, models.ExampleCreatedEvent) error {
	return nil
}

func BenchmarkModuleCreate(b *testing.B) {
	sut := module.NewExample(module.ExampleDeps{
		Repo:      &memRepo{rows: map[uuid.UUID]models.Example{}},
		Cache:     &memCache{},
		Publisher: nopPublisher{},
	})
	ctx := context.Background()
	input := models.Example{Name: "benchmark", OwnerID: "acc_1"}
	b.ReportAllocs()

	for b.Loop() {
		if _, err := sut.Create(ctx, input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkModuleGet(b *testing.B) {
	ex := models.Example{ID: uuid.New(), Name: "benchmark", OwnerID: "acc_1"}
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		hit  bool
	}{{"cache_hit", true}, {"cache_miss", false}} {
		b.Run(tc.name, func(b *testing.B) {
			sut := module.NewExample(module.ExampleDeps{
				Repo:  &memRepo{rows: map[uuid.UUID]models.Example{ex.ID: ex}},
				Cache: &memCache{hit: tc.hit, ex: ex},
			})
			b.ReportAllocs()
			for b.Loop() {
				if _, err := sut.Get(ctx, ex.ID); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
