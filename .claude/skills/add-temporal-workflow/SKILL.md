---
name: add-temporal-workflow
description: Add a Temporal workflow and activities to this Go service - deterministic workflow code, thin activities calling the module, error-to-retry mapping, worker registration, idempotent starting via a port, and testsuite tests. Use when asked for long-running, multi-step, retryable or scheduled background processing.
---

# Add a Temporal workflow

Reference files:
- `internal/handler/workflow/example.go` (workflow, activities, `toTemporalErr`, `Register`)
- `internal/handler/workflow/starter.go` (`WorkflowStarter` adapter)
- `internal/handler/workflow/example_test.go`

## Steps

1. **Workflow** in `internal/handler/workflow/<name>.go`:
   - Export a `const <Name>WorkflowName = "<Name>Workflow"` and use it for registration and starting.
   - `func <Name>Workflow(ctx workflow.Context, input models.X) error`
   - Set `workflow.ActivityOptions` (StartToCloseTimeout, RetryPolicy) via `workflow.WithActivityOptions`.
   - **Determinism rules.** Do not use any of these:
     - I/O, `time.Now`, `time.Sleep`, `rand`, goroutines, `select` on Go channels, or map iteration order.
   - Use `workflow.Now`, `workflow.Sleep`, `workflow.Go`, `workflow.NewSelector` and `workflow.SideEffect` instead.
   - Call activities with a nil receiver: `var a *Activities; workflow.ExecuteActivity(ctx, a.Step, arg).Get(ctx, nil)`.
   - Changing workflow logic after it is deployed requires `workflow.GetVersion` for in-flight executions.
2. **Activities**: methods on `Activities` (add fields for new modules). Keep them thin:
   - parse input;
   - call the module (core);
   - `return toTemporalErr(err)`.

   Business errors (invalid input, not found, forbidden, conflict) become **non-retryable**. Everything else is retried.
3. **Register** in `Register()` with `w.RegisterWorkflowWithOptions(fn, workflow.RegisterOptions{Name: <Name>WorkflowName})` and `w.RegisterActivity(acts)`. Update `InitiateWorker` if `Activities` gained fields.
4. **Start from the core**: add a method to the `WorkflowStarter` port (or a new port) in `internal/module`, then run `make mocks`. Implement it in `starter.go` with a **deterministic ID** (`"<name>-" + entityID`), `WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE` and `WorkflowExecutionErrorWhenAlreadyStarted: true`, and treat `*serviceerror.WorkflowExecutionAlreadyStarted` as success.
5. **Tests** in `<name>_test.go`:
   - `var s testsuite.WorkflowTestSuite; env := s.NewTestWorkflowEnvironment()`
   - `env.RegisterActivity(&workflow.Activities{X: mocks.NewX(t)})` with `EXPECT()` per activity
   - `env.ExecuteWorkflow(fn, input)`, then assert `IsWorkflowCompleted()` and `GetWorkflowError()`
   - Test the non-retryable path with `.Once()` on the mock and `errors.As(err, *temporal.ApplicationError)` → `NonRetryable()`.
6. Tracing is automatic: the client's OTel interceptor applies to the worker too.
7. **Benchmarks** (see `add-benchmark`): benchmark each activity as a plain method call with a stub module in `internal/handler/workflow/<name>_bench_test.go`, and each module method it calls in `internal/module`. Do not benchmark the workflow function.

Verify: `make lint test`. Locally, `make dev-up run-worker` and watch it in the Temporal UI (http://localhost:8233).
