---
name: add-temporal-workflow
description: Add a Temporal workflow and activities to this Go service - deterministic workflow code, thin activities calling the module, error-to-retry mapping, worker registration, idempotent starting via a port, and testsuite tests. Use when asked for long-running, multi-step, retryable or scheduled background processing.
---

# Add a Temporal workflow

There is **no workflow or activity in the repo yet**, and no `internal/handler/workflow` package. What exists:
- `internal/const/workflow/temporal/temporal.go` (`NewClient`, with the OTel interceptor)
- `initiator/platform.go` (`Platform.Temporal`, created only when `needs.Temporal` is set; both `apiNeeds` and `workerNeeds` leave it off today)
- `config.Temporal` (`host_port`, `namespace`, `task_queue: audit`) and the `temporal` service in `compose.dev.yml`

The steps below describe the pattern generically.

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
2. **Activities**: methods on an `Activities` struct whose fields are module ports. Keep them thin:
   - parse input;
   - call the module (core);
   - return the error through a `toTemporalErr` helper in the same package.

   `toTemporalErr` turns business errors (`ErrInvalidInput`, `ErrNotFound`, `ErrForbidden`, `ErrConflict`) into `temporal.NewNonRetryableApplicationError`. Everything else is retried.
3. **Register**: a `Register(w worker.Worker, acts *Activities)` function with `w.RegisterWorkflowWithOptions(fn, workflow.RegisterOptions{Name: <Name>WorkflowName})` and `w.RegisterActivity(acts)`. In `InitiateWorker`, set `Temporal: true` in `workerNeeds`, create a `worker.New(p.Temporal, cfg.Temporal.TaskQueue, ...)`, call `Register`, and run it in the errgroup.
4. **Start from the core**: add a `WorkflowStarter`-style port in `internal/module`, then run `make mocks`. Implement it in `internal/handler/workflow/starter.go` with a **deterministic ID** (`"<name>-" + entityID`), `WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE` and `WorkflowExecutionErrorWhenAlreadyStarted: true`, and treat `*serviceerror.WorkflowExecutionAlreadyStarted` as success. The starting process needs `Temporal: true` too.
5. **Tests** in `<name>_test.go`:
   - `var s testsuite.WorkflowTestSuite; env := s.NewTestWorkflowEnvironment()`
   - `env.RegisterActivity(&workflow.Activities{X: mocks.NewX(t)})` with `EXPECT()` per activity
   - `env.ExecuteWorkflow(fn, input)`, then assert `IsWorkflowCompleted()` and `GetWorkflowError()`
   - Test the non-retryable path with `.Once()` on the mock and `errors.As(err, *temporal.ApplicationError)` → `NonRetryable()`.
6. Tracing is automatic: the client's OTel interceptor applies to the worker too.
7. **Benchmarks** (see `add-benchmark`): benchmark each activity as a plain method call with a stub module in `internal/handler/workflow/<name>_bench_test.go`, and each module method it calls in `internal/module` (like `module/audit_bench_test.go`). Do not benchmark the workflow function.

Verify: `make lint test`. Locally, `make dev-up run-worker` and watch it in the Temporal UI (http://localhost:8233).
