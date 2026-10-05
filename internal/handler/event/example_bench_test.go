package event_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/handler/event"
	"github.com/hunderaweke/dps-audit-service/internal/module"
)

// stubExample is a zero-overhead module so the benchmark measures the Kafka
// adapter (JSON decoding and dispatch), not the core.
type stubExample struct{ module.Example }

func (stubExample) HandleCreated(context.Context, models.ExampleCreatedEvent) error { return nil }

func BenchmarkExampleCreatedHandler(b *testing.B) {
	payload, err := json.Marshal(models.ExampleCreatedEvent{ID: uuid.New(), OwnerID: "acc_1"})
	if err != nil {
		b.Fatal(err)
	}
	rec := &kgo.Record{Topic: "example.created", Value: payload}
	handle := event.ExampleCreated(stubExample{})
	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		if err := handle(ctx, rec); err != nil {
			b.Fatal(err)
		}
	}
}
