---
name: add-migration-query
description: Change the Postgres schema or add/modify SQL queries in this Go service using golang-migrate and sqlc. Use when asked to add a table, column, index or query, or when repository code needs a new database operation.
---

# Add a migration and/or query

Schema files live in `internal/const/migrations/`. They are golang-migrate files that are embedded into the binary (`migrations.go`) and also serve as the sqlc schema (`sqlc.yaml`).
Queries live in `internal/const/queries/*.sql`. Generated Go goes to `internal/storage/repository/db/`, which you **never edit**.
Reference: `000001_audit.{up,down}.sql` (the only migration: table `audit_records`, `event_id` PK, three indexes, and the `audit_records_append_only` trigger that refuses UPDATE and DELETE) and `queries/audit.sql` (`InsertAuditRecords :execrows`, a batch insert over `unnest` arrays with `ON CONFLICT (event_id) DO NOTHING`; `GetAuditRecord :one`).
Production runs YugabyteDB YSQL; dev and tests run Postgres 17. Write plain Postgres-compatible SQL.

## Schema change

1. `make migrate-new NAME=<snake_case_description>` creates `00000N_<name>.up.sql` and `.down.sql`.
2. Write the `up` SQL. Prefer additive, backward-compatible changes: add nullable columns or columns with defaults, and `CREATE INDEX CONCURRENTLY` for big tables (you cannot run it in a transaction, so put it in its own migration).
3. Write a `down` that exactly reverts `up`.
4. Never edit a migration that has already been applied anywhere shared. Add a new one instead.
5. `audit_records` is append-only: never add a migration that updates or deletes its rows, and keep the trigger.
6. Apply locally by restarting `make run-api` (it auto-migrates when `postgres.auto_migrate` is on) or with the compose `migrate` job. `make migrate-up` currently fails (the cached `go tool migrate` binary has no `pgx5` driver). If a migration fails halfway, fix the SQL, then `make migrate-force V=<last good>`.

## Query change

1. Add the annotated query to `internal/const/queries/<domain>.sql`:
   ```sql
   -- name: ListAuditRecordsByAggregate :many
   SELECT * FROM audit_records WHERE aggregate_id = $1 ORDER BY occurred_at DESC LIMIT $2;
   ```
   Use `:one`, `:many`, `:exec` or `:execrows`. Use `@name`, `sqlc.arg(name)` or `sqlc.narg(name)` for named or nullable params. Make sure an index serves the query (here `(aggregate_id, occurred_at DESC)`).
2. Run `make sqlc`. It fails loudly on SQL errors against the migrations schema.
3. Use the new `db.Querier` method in the repository adapter (`internal/storage/repository/audit.go` or a new `<domain>.go`). Map the row to the model (`auditToModel`) and the errors to `apperrors` (`ErrNotFound`, `ErrDBRead`, `ErrDBWrite`).
4. If the module needs it, add the method to the outbound port in `internal/module`, then run `make mocks`.
5. Type overrides (uuid → `uuid.UUID`, timestamptz → `time.Time`) are set in `sqlc.yaml`. Add more there if needed.

## Transactions

`repository.NewAudit` takes `db.Querier`, so the same adapter works inside a transaction:
```go
tx, _ := pool.Begin(ctx); defer tx.Rollback(ctx)
log := repository.NewAudit(db.New(tx))
// ... log.Append(ctx, recs) ...
tx.Commit(ctx)
```
To expose this to the core, add a `UnitOfWork`/`TxRunner` port instead of passing `pgx.Tx` into the module.

Verify: `make sqlc lint test`, plus `TestAuditRepository` / `BenchmarkAuditRepository` in `repository/audit_test.go` (docker) or their equivalent for a new adapter if the query is hot.
