---
name: add-endpoint
description: Add or change an HTTP endpoint in this Go service using Huma v2 on gin - DTOs with validation tags, operation registration, error mapping, OpenAPI, router tests and a Gherkin e2e scenario. Use when asked to add a REST route, change a request/response shape, or expose a use case over HTTP.
---

# Add an HTTP endpoint (Huma + gin)

Reference files: `internal/const/dto/example.go`, `internal/router/example.go`, `internal/router/errors.go`, `internal/router/example_test.go`.

## Steps

1. **DTOs** in `internal/const/dto/<domain>.go`:
   - Input struct fields:
     - `path:"id"`, `query:"limit"` or `header:"X-..."`;
     - a `Body` field for JSON;
     - Huma validation tags: `minLength`, `maxLength`, `minimum`, `maximum`, `pattern`, `enum`, `format`, `default`;
     - `doc` / `example` for the docs.
   - Output struct: `Body <Type>` plus optional header fields.
   - Conversion helpers to and from models. DTOs must not leak into the module.
2. **Core**: if this is a new use case, add the method to the inbound port interface in `internal/module/<domain>.go`, implement it, and run `make mocks`.
3. **Register** in `internal/router/<domain>.go`:
   ```go
   huma.Register(api, huma.Operation{
       OperationID: "verb-noun", Method: http.MethodPost, Path: "/v1/things/{id}/action",
       Summary: "...", Tags: []string{"Things"}, DefaultStatus: http.StatusCreated, // when not 200
   }, h.action)
   ```
   The handler signature is `func(ctx context.Context, in *dto.XRequest) (*dto.XResponse, error)`.
4. **Errors**: always `return nil, h.errs.toHuma(ctx, err)`. Status comes from `apperrors.HTTPStatus`, and 5xx details are logged, never returned. For a new status category, add an errorx type and a case in `HTTPStatus`.
5. **Routes**: new `Register*` functions must be called from `registerRoutes` in `initiator/handler.go`.
6. **Tests** in `internal/router/<domain>_test.go`, using `newServer(mock)` and `do(...)`:
   - success status and body;
   - Huma validation returns 422 with **no** mock expectations;
   - each mapped error status.
7. **E2E**: add a scenario to `tests/e2e/features/<domain>_rest.feature`. Reuse the steps in `tests/e2e/steps_test.go` or add new ones there.
8. **Docs**: `make openapi` updates `docs/openapi.yaml`. You can browse it at `http://localhost:8080/docs`.
9. **Benchmark**: add a router benchmark in `internal/router/<domain>_bench_test.go` (`newServer(stub)`, check the status inside the loop). If the module method is new, add a core benchmark too. See `add-benchmark`.
10. **Load**: if the endpoint is user-facing, add it to `tests/load/example.js` with a `tags: { name: '...' }` and a threshold.

Verify: `make lint test test-e2e openapi`.
