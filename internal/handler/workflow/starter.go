package workflow

import (
	"context"
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	apperrors "github.com/username/example-service/internal/const/errors"
	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/module"
)

type starter struct {
	client    client.Client
	taskQueue string
}

var _ module.WorkflowStarter = (*starter)(nil)

func NewStarter(c client.Client, taskQueue string) module.WorkflowStarter {
	return &starter{client: c, taskQueue: taskQueue}
}

// StartProcessExample is idempotent: the workflow ID is derived from the
// example ID, so a redelivered Kafka event does not start a second workflow.
func (s *starter) StartProcessExample(ctx context.Context, ev models.ExampleCreatedEvent) error {
	_, err := s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       "process-example-" + ev.ID.String(),
		TaskQueue:                                s.taskQueue,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, ProcessExampleWorkflowName, ev)

	var started *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &started) {
		return nil
	}
	if err != nil {
		return apperrors.ErrUnavailable.Wrap(err, "start workflow")
	}
	return nil
}
