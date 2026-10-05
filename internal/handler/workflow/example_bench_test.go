package workflow_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/handler/workflow"
	"github.com/hunderaweke/dps-audit-service/internal/module"
)

// stubExample is a zero-overhead module so the benchmarks measure the
// activity adapter (ID parsing, error-to-retry mapping), not the core.
type stubExample struct{ module.Example }

func (stubExample) VerifyOwner(context.Context, uuid.UUID) error { return nil }

func (stubExample) MarkProcessed(_ context.Context, id uuid.UUID) (models.Example, error) {
	return models.Example{ID: id, Status: models.ExampleStatusProcessed}, nil
}

// Activities are benchmarked as plain method calls. The Temporal test
// environment adds a scheduler per run, which would dominate the numbers.
func BenchmarkActivities(b *testing.B) {
	acts := &workflow.Activities{Example: stubExample{}}
	id := uuid.NewString()
	ctx := context.Background()

	b.Run("verify_owner", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := acts.VerifyOwner(ctx, id); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("mark_processed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := acts.MarkProcessed(ctx, id); err != nil {
				b.Fatal(err)
			}
		}
	})
}
