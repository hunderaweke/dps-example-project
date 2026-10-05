package workflow_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	apperrors "github.com/hunderaweke/dps-audit-service/internal/const/errors"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/handler/workflow"
	"github.com/hunderaweke/dps-audit-service/internal/module/mocks"
)

func TestProcessExampleWorkflow(t *testing.T) {
	ev := models.ExampleCreatedEvent{ID: uuid.New(), OwnerID: "acc_1"}

	t.Run("verifies owner then marks processed", func(t *testing.T) {
		var s testsuite.WorkflowTestSuite
		env := s.NewTestWorkflowEnvironment()

		example := mocks.NewExample(t)
		example.EXPECT().VerifyOwner(mock.Anything, ev.ID).Return(nil)
		example.EXPECT().MarkProcessed(mock.Anything, ev.ID).Return(models.Example{ID: ev.ID}, nil)
		env.RegisterActivity(&workflow.Activities{Example: example})

		env.ExecuteWorkflow(workflow.ProcessExampleWorkflow, ev)

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
	})

	t.Run("forbidden owner fails without retries", func(t *testing.T) {
		var s testsuite.WorkflowTestSuite
		env := s.NewTestWorkflowEnvironment()

		example := mocks.NewExample(t)
		example.EXPECT().VerifyOwner(mock.Anything, ev.ID).Return(apperrors.ErrForbidden.New("inactive")).Once()
		env.RegisterActivity(&workflow.Activities{Example: example})

		env.ExecuteWorkflow(workflow.ProcessExampleWorkflow, ev)

		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.Error(t, err)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		assert.True(t, appErr.NonRetryable())
	})
}
