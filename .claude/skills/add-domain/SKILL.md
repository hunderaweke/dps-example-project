---
name: add-domain
description: Scaffold a new domain entity (e.g. Order, Invoice) across every hexagonal layer of this Go service - migration, sqlc queries, model, module ports and use cases, storage adapters, DTOs, Huma routes, initiator wiring and tests. Use when asked to add a new resource, entity, aggregate or CRUD feature.
---

# Add a new domain

Use the `Example` slice as the canonical reference and mirror its files. Replace `Order`/`order` with the new name.
Read the `architecture-rules` skill first.

## Steps

1. **Migration**: `make migrate-new NAME=create_orders`. Write the `up` (CREATE TABLE ... plus indexes) and `down` (DROP TABLE) files in `internal/const/migrations/`.
2. **Queries**: create `internal/const/queries/order.sql` with `-- name: CreateOrder :one`, `GetOrder :one`, `ListOrders :many`, and so on. Run `make sqlc`.
3. **Model**: `internal/const/models/order.go` with the entity, a status type if needed, events (`OrderCreatedEvent`), and `validate:"..."` tags. No internal imports.
4. **Core**: `internal/module/order.go`. Copy the structure of `module/example.go`:
   - an inbound port `type Order interface { ... }`;
   - outbound ports (`OrderRepository`, and `OrderCache` / publisher only if needed);
   - `type OrderDeps struct { ...; Logger *zap.Logger }`, and `func NewOrder(deps OrderDeps) Order` that sets up `validator.New(validator.WithRequiredStructEnabled())`;
   - methods that return `apperrors` types; validate with `ErrInvalidInput`.

   Run `make mocks`.
5. **Repository adapter**: `internal/storage/repository/order.go` with:
   - `type order struct{ q db.Querier }`, plus `var _ module.OrderRepository = (*order)(nil)`;
   - `pgx.ErrNoRows` mapped to `apperrors.ErrNotFound`, and other errors to `ErrDBRead` / `ErrDBWrite`;
   - a `toModel` mapping (name it `orderToModel` to avoid clashing with the existing `toModel`).
6. **Other adapters** (optional): cache in `storage/cache/order.go`, publisher in `storage/publisher/order.go`.
7. **DTOs**: `internal/const/dto/order.go` with request structs (`Body`, `path:`, `query:` fields and Huma validation tags), response structs, `ToModel()` and `OrderFromModel()`.
8. **Routes**: `internal/router/order.go` with `func RegisterOrder(api huma.API, m module.Order, logger *zap.Logger)`. Use `huma.Register` with `OperationID`, `Method`, `Path` (`/v1/orders`), `Summary` and `Tags`. Return errors through `errorMapper{logger}.toHuma(ctx, err)`.
9. **Wiring** in `initiator/`:
   - `module.go`: add `Order module.Order` to `Modules` and build its deps inside `newModules` (guard each adapter on its platform client being non-nil).
   - `handler.go`: call `router.RegisterOrder(api, mods.Order, logger.Named("router"))` in `registerRoutes`.
10. **Tests**:
    - `internal/module/order_test.go`: table of cases with `mocks.New...(t)` and `EXPECT()`.
    - `internal/router/order_test.go`: status codes, and a check that 422 never reaches the module.
    - `tests/e2e/features/order_rest.feature` plus new steps in `tests/e2e/steps_test.go`.
11. **Benchmarks** (see `add-benchmark`): `internal/module/order_bench_test.go` with in-memory fakes for each use case, `internal/router/order_bench_test.go` for each operation, and k6 requests with thresholds in `tests/load/`.
12. Verify: `make generate lint test test-e2e`. Then `make openapi` and check the new operations in `docs/openapi.yaml`.
