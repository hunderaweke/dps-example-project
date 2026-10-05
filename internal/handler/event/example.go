package event

import (
	"context"
	"encoding/json"

	"github.com/twmb/franz-go/pkg/kgo"

	apperrors "github.com/hunderaweke/dps-audit-service/internal/const/errors"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
)

// ExampleCreated decodes example.created records and hands them to the core.
func ExampleCreated(example module.Example) HandlerFunc {
	return func(ctx context.Context, rec *kgo.Record) error {
		var ev models.ExampleCreatedEvent
		if err := json.Unmarshal(rec.Value, &ev); err != nil {
			return apperrors.ErrInvalidInput.Wrap(err, "decode example.created")
		}
		return example.HandleCreated(ctx, ev)
	}
}
