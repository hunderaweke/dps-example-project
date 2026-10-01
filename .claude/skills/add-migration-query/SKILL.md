---
name: add-migration-query
description: Change the Postgres schema or add/modify SQL queries in this Go service using golang-migrate and sqlc. Use when asked to add a table, column, index or query, or when repository code needs a new database operation.
---

# Add a migration and/or query

Schema files live in `internal/const/migrations/`. They are golang-migrate files that are embedded into the binary and also serve as the sqlc schema.
Queries live in `internal/const/queries/*.sql`. Generated Go goes to `internal/storage/repository/db/`, which you **never edit**.

## Schema change

1. `make migrate-new NAME=<snake_case_description>` creates `00000N_<name>.up.sql` and `.down.sql`.
2. Write the `up` SQL. Prefer additive, backward-compatible changes: add nullable columns or columns with defaults, and `CREATE INDEX CONCURRENTLY` for big tables (you cannot run it in a transaction, so put it in its own migration).
3. Write a `down` that exactly reverts `up`.
4. Never edit a migration that has already been applied anywhere shared. Add a new one instead.
5. Apply locally with `make migrate-up` (or restart `make run-api`, which auto-migrates in development). If it fails halfway, fix the SQL, then `make migrate-force V=<last good>`.

## Query change

1. Add the annotated query to `internal/const/queries/<domain>.sql`:
   ```sql
   -- name: GetOrdersByCustomer :many
   SELECT * FROM orders WHERE customer_id = $1 ORDER BY created_at DESC LIMIT $2;
   ```
   Use `:one`, `:many`, `:exec` or `:execrows`. Use `sqlc.arg(name)` or `sqlc.narg(name)` for named or nullable params.
2. Run `make sqlc`. It fails loudly on SQL errors against the migrations schema.
3. Use the new `db.Querier` method in the repository adapter (`internal/storage/repository/`). Map the row to the model and the errors to `apperrors`.
4. If the module needs it, add the method to the outbound port in `internal/module`, then run `make mocks`.
5. Type overrides (uuid → `uuid.UUID`, timestamptz → `time.Time`) are set in `sqlc.yaml`. Add more there if needed.

## Transactions

`repository.NewExample` takes `db.Querier`, so the same adapter works inside a transaction:
```go
tx, _ := pool.Begin(ctx); defer tx.Rollback(ctx)
repo := repository.NewExample(db.New(tx))
// ... repo calls ...
tx.Commit(ctx)
```
To expose this to the core, add a `UnitOfWork`/`TxRunner` port instead of passing `pgx.Tx` into the module.

Verify: `make sqlc lint test`, plus an adapter benchmark or test if the query is hot.
