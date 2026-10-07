---
name: add-domain
description: Scaffold a new domain entity (e.g. Order, Invoice) across every hexagonal layer of this Go service - migration, sqlc queries, model, module ports and use cases, storage adapters, DTOs, Huma routes, initiator wiring and tests. Use when asked to add a new resource, entity, aggregate or CRUD feature.
---

# Add a new domain

Use the `Audit` slice as the canonical reference and mirror its files: `models/audit.go`, `module/audit.go` (+ `audit_test.go`, `audit_bench_test.go`), `repository/audit.go` (+ `audit_test.go`), `migrations/000001_audit.up.sql`, `queries/audit.sql`. Replace `Order`/`order` with the new name.
Read the `architecture-rules` skill first. The audit slice has no HTTP routes yet, so steps 7–8 follow `add-endpoint`.

## Steps

1. **Migration**: `make migrate-new NAME=create_orders`. Write the `up` (CREATE TABLE ... plus indexes) and `down` (DROP TABLE) files in `internal/const/migrations/`. Keep the SQL plain Postgres so it also runs on YugabyteDB YSQL.
2. **Queries**: create `internal/const/queries/order.sql` with `-- name: CreateOrder :one`, `GetOrder :one`, `ListOrders :many`, and so on. Run `make sqlc`.
3. **Model**: `internal/const/models/order.go` with the entity, a status type if needed, and `validate:"..."` tags. No internal imports.
4. **Core**: `internal/module/order.go`. Copy the structure of `module/audit.go`:
   - an inbound port `type Order interface { ... }`;
   - outbound ports (`OrderRepository`, plus a cache or publisher port only if needed);
   - `type OrderDeps struct { ...; Logger *zap.Logger }`, and `func NewOrder(deps OrderDeps) Order` that defaults a nil logger and sets up `validator.New(validator.WithRequiredStructEnabled())`;
   - methods that return `apperrors` types; validate with `ErrInvalidInput`.

   Run `make mocks`.
5. **Repository adapter**: `internal/storage/repository/order.go` with:
   - `type order struct{ q db.Querier }`, plus `var _ module.OrderRepository = (*order)(nil)`;
   - `pgx.ErrNoRows` mapped to `apperrors.ErrNotFound`, and other errors to `ErrDBRead` / `ErrDBWrite`;
   - a `orderToModel` mapping (the audit adapter already owns `auditToModel` and `toInsertParams`).
6. **Other adapters** (optional, none in the repo yet): a cache in `storage/cache/order.go` (Valkey) or a publisher in `storage/publisher/order.go` (franz-go). Each implements a port from step 4 and wraps errors in `ErrCache` / `ErrPublish`.
7. **DTOs**: `internal/const/dto/order.go` with request structs (`Body`, `path:`, `query:` fields and Huma validation tags), response structs, `ToModel()` and `OrderFromModel()`.
8. **Routes**: `internal/router/order.go` with `func RegisterOrder(api huma.API, m module.Order, logger *zap.Logger)`. Use `huma.Register` with `OperationID`, `Method`, `Path` (`/v1/orders`), `Summary` and `Tags`. Return errors through `errorMapper{logger}.toHuma(ctx, err)`.
9. **Wiring** in `initiator/`:
   - `module.go`: add `Order module.Order` to `Modules` and build its deps inside `newModules`, guarding each adapter on its platform client being non-nil, as `Audit` does with `p.Postgres`.
   - `handler.go`: call `router.RegisterOrder(api, mods.Order, logger.Named("router"))` in `registerRoutes` (empty today).
   - `platform.go`: enable any extra client (Valkey, Producer, Temporal) in `apiNeeds` / `workerNeeds`; both are Postgres only today.
10. **Tests**:
    - `internal/module/order_test.go`: cases with `mocks.New...(t)` and `EXPECT()`, as in `module/audit_test.go`.
    - `internal/storage/repository/order_test.go`: testcontainers Postgres, skipped with `-short`, as in `TestAuditRepository`.
    - `internal/router/order_test.go`: status codes, and a check that 422 never reaches the module.
    - `tests/e2e/features/order_rest.feature` plus new steps in `tests/e2e/steps_test.go`.
11. **Benchmarks** (see `add-benchmark`): `internal/module/order_bench_test.go` with in-memory fakes (like `nopLog`), `BenchmarkOrderRepository` next to the adapter test, and `internal/router/order_bench_test.go` for each operation.
12. Verify: `make generate lint test test-e2e`. Then `make openapi` and check the new operations in `docs/openapi.yaml`.
