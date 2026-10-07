---
name: add-endpoint
description: Add or change an HTTP endpoint in this Go service using Huma v2 on gin - DTOs with validation tags, operation registration, error mapping, OpenAPI, router tests and a Gherkin e2e scenario. Use when asked to add a REST route, change a request/response shape, or expose a use case over HTTP.
---

# Add an HTTP endpoint (Huma + gin)

Reference files: `internal/const/dto/health.go`, `internal/router/health.go` (`RegisterHealth`), `internal/router/errors.go` (`errorMapper`), `initiator/handler.go` (`registerRoutes`), `tests/e2e/features/health.feature`.
There is **no business endpoint in the repo yet**: only `/healthz` and `/readyz`, and `registerRoutes` is empty. The planned first one is the read-only audit API (`dps.audit.v1.AuditService`) over `module.Audit`. The steps below describe the pattern generically.

## Steps

1. **DTOs** in `internal/const/dto/<domain>.go`:
   - Input struct fields:
     - `path:"id"`, `query:"limit"` or `header:"X-..."`;
     - a `Body` field for JSON;
     - Huma validation tags: `minLength`, `maxLength`, `minimum`, `maximum`, `pattern`, `enum`, `format`, `default`;
     - `doc` / `example` for the docs.
   - Output struct: `Body <Type>` plus optional header fields (see `dto.HealthResponse`).
   - Conversion helpers to and from models. DTOs must not leak into the module.
2. **Core**: if this is a new use case, add the method to the inbound port interface in `internal/module/<domain>.go`, implement it, and run `make mocks`.
3. **Register** in `internal/router/<domain>.go`, with a `Register<Domain>(api huma.API, m module.X, logger *zap.Logger)` function:
   ```go
   huma.Register(api, huma.Operation{
       OperationID: "get-audit-record", Method: http.MethodGet, Path: "/v1/audit-records/{eventId}",
       Summary: "...", Tags: []string{"Audit"}, DefaultStatus: http.StatusOK, // set when not 200
   }, h.get)
   ```
   The handler signature is `func(ctx context.Context, in *dto.XRequest) (*dto.XResponse, error)`.
4. **Errors**: always `return nil, h.errs.toHuma(ctx, err)` with an `errorMapper{logger}`. Remove its `//nolint:unused` markers once it has a caller. Status comes from `apperrors.HTTPStatus`, and 5xx details are logged, never returned. For a new status category, add an errorx type and a case in `HTTPStatus`.
5. **Routes**: new `Register*` functions must be called from `registerRoutes` in `initiator/handler.go` (it is also used by `OpenAPI()`).
6. **Tests** in `internal/router/<domain>_test.go`: build a Huma test API (`humatest.New`) with the route registered against a mockery mock of the port, then check:
   - success status and body;
   - Huma validation returns 422 with **no** mock expectations;
   - each mapped error status.
7. **E2E**: add a scenario to `tests/e2e/features/<domain>.feature`. Reuse the steps in `tests/e2e/steps_test.go` (`I request "GET" "/path"`, `the response status should be N`, `the response field ... should be ...`) or add new ones there.
8. **Docs**: `make openapi` updates `docs/openapi.yaml`. You can browse it at `http://localhost:8080/docs`.
9. **Benchmark**: add a router benchmark in `internal/router/<domain>_bench_test.go` (the same test API with a hand-written stub module; check the status inside the loop). If the module method is new, add a core benchmark too, like `module/audit_bench_test.go`. See `add-benchmark`.
10. **Load**: if the endpoint is user-facing, add a k6 script with a `tags: { name: '...' }` and a threshold (there is no load-test script in the repo yet; see `add-benchmark`).

Verify: `make lint test test-e2e openapi`.
