package workflow

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/joomcode/errorx"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	apperrors "github.com/username/example-service/internal/const/errors"
	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/module"
)

const ProcessExampleWorkflowName = "ProcessExampleWorkflow"

// Workflow code must be deterministic: no I/O, time.Now, or rand here; do
// that in activities.
func ProcessExampleWorkflow(ctx workflow.Context, ev models.ExampleCreatedEvent) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	})

	var a *Activities // nil receiver: only used to reference the methods by name
	if err := workflow.ExecuteActivity(ctx, a.VerifyOwner, ev.ID.String()).Get(ctx, nil); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, a.MarkProcessed, ev.ID.String()).Get(ctx, nil)
}

type Activities struct {
	Example module.Example
}

func (a *Activities) VerifyOwner(ctx context.Context, id string) error {
	uid, err := uuid.Parse(id)
	if err != nil {
		return temporal.NewNonRetryableApplicationError("invalid example id", "invalid_input", err)
	}
	return toTemporalErr(a.Example.VerifyOwner(ctx, uid))
}

func (a *Activities) MarkProcessed(ctx context.Context, id string) error {
	uid, err := uuid.Parse(id)
	if err != nil {
		return temporal.NewNonRetryableApplicationError("invalid example id", "invalid_input", err)
	}
	_, err = a.Example.MarkProcessed(ctx, uid)
	return toTemporalErr(err)
}

// toTemporalErr marks business errors as non-retryable so Temporal does not
// retry something that can never succeed. Everything else is retried.
func toTemporalErr(err error) error {
	if err == nil {
		return nil
	}
	for _, t := range []*errorx.Type{apperrors.ErrInvalidInput, apperrors.ErrNotFound, apperrors.ErrForbidden, apperrors.ErrConflict} {
		if errorx.IsOfType(err, t) {
			return temporal.NewNonRetryableApplicationError(err.Error(), t.FullName(), err)
		}
	}
	return err
}

func Register(w worker.Registry, acts *Activities) {
	w.RegisterWorkflowWithOptions(ProcessExampleWorkflow, workflow.RegisterOptions{Name: ProcessExampleWorkflowName})
	w.RegisterActivity(acts)
}
