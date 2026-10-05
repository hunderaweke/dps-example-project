package module_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
)

// In-memory fakes keep the benchmark focused on business logic (validation,
// ID generation, cache-aside flow) rather than mock framework overhead.
type memRepo struct {
	module.ExampleRepository
	mu   sync.RWMutex
	rows map[uuid.UUID]models.Example
	list []models.Example
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

func (r *memRepo) List(_ context.Context, page models.Page) ([]models.Example, error) {
	return r.list[:min(int(page.Limit), len(r.list))], nil
}

func (r *memRepo) UpdateStatus(_ context.Context, id uuid.UUID, status models.ExampleStatus) (models.Example, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ex := r.rows[id]
	ex.Status = status
	r.rows[id] = ex
	return ex, nil
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
func (c *memCache) Delete(context.Context, uuid.UUID) error   { return nil }

type nopPublisher struct{}

func (nopPublisher) PublishExampleCreated(context.Context, models.ExampleCreatedEvent) error {
	return nil
}

type nopStarter struct{}

func (nopStarter) StartProcessExample(context.Context, models.ExampleCreatedEvent) error {
	return nil
}

type activeAccounts struct{}

func (activeAccounts) GetAccount(_ context.Context, id string) (models.Account, error) {
	return models.Account{ID: id, Name: "bench", Active: true}, nil
}

type nopAudit struct{}

func (nopAudit) Record(context.Context, models.AuditEntry) error { return nil }

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

func BenchmarkModuleList(b *testing.B) {
	list := make([]models.Example, 100)
	for i := range list {
		list[i] = models.Example{ID: uuid.New(), Name: "benchmark", OwnerID: "acc_1"}
	}
	sut := module.NewExample(module.ExampleDeps{Repo: &memRepo{list: list}})
	ctx := context.Background()

	for _, limit := range []int32{20, 100} {
		b.Run(fmt.Sprintf("limit_%d", limit), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				items, err := sut.List(ctx, models.Page{Limit: limit})
				if err != nil {
					b.Fatal(err)
				}
				if len(items) != int(limit) {
					b.Fatalf("got %d items, want %d", len(items), limit)
				}
			}
		})
	}
}

// The benchmarks below cover the worker path: Kafka event -> workflow start ->
// activities. Each one measures one use case called by an inbound adapter.

func BenchmarkModuleHandleCreated(b *testing.B) {
	sut := module.NewExample(module.ExampleDeps{Workflows: nopStarter{}})
	ctx := context.Background()
	ev := models.ExampleCreatedEvent{ID: uuid.New(), OwnerID: "acc_1"}
	b.ReportAllocs()

	for b.Loop() {
		if err := sut.HandleCreated(ctx, ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkModuleVerifyOwner(b *testing.B) {
	ex := models.Example{ID: uuid.New(), Name: "benchmark", OwnerID: "acc_1"}
	sut := module.NewExample(module.ExampleDeps{
		Repo:     &memRepo{rows: map[uuid.UUID]models.Example{ex.ID: ex}},
		Accounts: activeAccounts{},
	})
	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		if err := sut.VerifyOwner(ctx, ex.ID); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkModuleMarkProcessed(b *testing.B) {
	ex := models.Example{ID: uuid.New(), Name: "benchmark", OwnerID: "acc_1", Status: models.ExampleStatusPending}
	sut := module.NewExample(module.ExampleDeps{
		Repo:  &memRepo{rows: map[uuid.UUID]models.Example{ex.ID: ex}},
		Cache: &memCache{},
		Audit: nopAudit{},
	})
	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		if _, err := sut.MarkProcessed(ctx, ex.ID); err != nil {
			b.Fatal(err)
		}
	}
}
