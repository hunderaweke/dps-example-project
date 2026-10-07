# dps-audit-service

The DPS audit service: a Go 1.27 service using **hexagonal architecture** (ports and adapters). It consumes every DPS domain event from Redpanda and keeps one append-only record per event. Read `README.md` §2 for the full architecture and `docs/architecture.md` for runtime flows, deployment and the roadmap.

## Must know

- Core = `internal/module` (ports + use cases; `audit.go` is the reference slice). Domain = `internal/const/models`. Event catalog = `internal/const/events`. Adapters:
  - inbound: `internal/router` (health only so far), `internal/handler/event` (Kafka; no handlers registered yet);
  - outbound: `internal/storage/repository` (Postgres/YugabyteDB YSQL via sqlc).
- Composition root = `initiator/`, the only place that knows concrete types.
- Import rules are enforced by depguard: **run `make lint` after every change.**
- Never edit generated code:
  - `internal/storage/repository/db` (`make sqlc`)
  - `internal/module/mocks` (`make mocks`)
  - `pkg/dpsapi/gen` (`make proto`, from dps-contracts)
- Errors: adapters wrap driver errors into `internal/const/errors` types. The router maps them with `HTTPStatus`.
- Validation: Huma tags at the edge, `validator` tags and checks in the core.
- Benchmarks: a new or changed use case, endpoint, Kafka handler, activity or storage adapter ships with a `b.Loop` benchmark in the same change. Each benchmark case carries a `perf.Budget`; run `make bench-check` before finishing a perf-sensitive change. The rules are in the `add-benchmark` skill.

## Skills (`.claude/skills/`)

`architecture-rules` (read first), `add-domain`, `add-migration-query`, `add-endpoint`, `add-kafka-consumer`, `add-temporal-workflow`, `add-grpc-client`, `add-benchmark`.

## Commands

```bash
make help                 # everything
make dev-up               # local infra
make run-api / run-worker
make generate             # sqlc + proto + mocks + openapi
make lint test            # before finishing any change
make test-e2e             # godog + testcontainers (docker)
make test-integration     # audit consumer vs Redpanda + Postgres (docker)
make bench-compare        # benchstat vs bench/baseline.txt
make bench-check          # fail on broken benchmark budgets (bench-check-all adds docker ones)
```
