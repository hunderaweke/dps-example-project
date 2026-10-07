---
name: architecture-rules
description: Hexagonal architecture rules for this Go service: what each folder is responsible for and what it may or must not import. Use before creating or editing any Go file under internal/, pkg/, initiator/ or cmd/, and when deciding where new code belongs.
---

# Architecture rules (hexagonal / ports and adapters)

Dependencies point **inward**: adapters → core → domain. Only `initiator/` knows concrete types.
These rules are enforced by `depguard` in `.golangci.yml`. Always run `make lint` after changes.

## Where does my code go?

| If the code... | Put it in | Reference file |
|---|---|---|
| is an entity, value object or event payload | `internal/const/models/` | `models/audit.go` |
| is a DPS event name, topic or header | `internal/const/events/<domain>.go` | `events/events.go`, `events/ledger.go` |
| is an error category | `internal/const/errors/errors.go` (reuse existing types first) | |
| is business logic / a use case | `internal/module/<domain>.go` | `module/audit.go` |
| is an interface the core needs (DB, cache, broker, external API) | **outbound port** in `internal/module/<domain>.go` | `AuditLog` |
| reads/writes Postgres (YugabyteDB YSQL in production) | `internal/storage/repository/<domain>.go` via sqlc `db.Querier` | `repository/audit.go` |
| touches Valkey | `internal/storage/cache/<domain>.go` | none yet |
| produces Kafka records | `internal/storage/publisher/<domain>.go` | none yet |
| wraps an external gRPC SDK as a port | `internal/storage/<service>/` | none yet |
| is generated contract code or a reusable SDK with no internal deps | `pkg/<name>/` | `pkg/dpsapi/gen` (generated) |
| handles HTTP | `internal/router/<domain>.go` + DTOs in `internal/const/dto/` | `router/health.go`, `dto/health.go` |
| consumes Kafka | `internal/handler/event/<domain>.go` | `handler/event/consumer.go` (loop only) |
| is a Temporal workflow/activity | `internal/handler/workflow/<domain>.go` | none yet |
| connects to infrastructure (pool, client, ping) | `internal/const/{database,cache,messaging,workflow}/` | `database/postgres/postgres.go` |
| wires things together | `initiator/` (`platform.go`, `module.go`, `handler.go`, `initiator.go`) | |

"None yet" means the folder does not exist; follow the conventions below and the matching `add-*` skill.

## Import rules (hard)

- `internal/const/models` must import **no** internal packages and no infrastructure libraries.
- `internal/module` (core) must **not** import any of these:
  - gin, huma, pgx, go-redis, franz-go, go.temporal.io, grpc (and any other database driver);
  - `internal/storage`, `internal/router`, `internal/handler`, `pkg/`, `initiator`.

  It may use `models`, `errors`, `validator`, `zap` and `uuid`.
- `internal/router` and `internal/handler/**` must **not** import `internal/storage`. They call the module through its port.
- `pkg/**` must **not** import `internal/**`.
- Only `cmd/*` imports `initiator`. The one exception is `tests/e2e`, which uses `initiator.BuildAPI`.
- `internal/const/dto` must **not** import `internal/storage`, `internal/const/{database,cache,messaging,workflow}` or any infrastructure driver.
- `internal/const/{database,cache,messaging,workflow}` may be imported only by `initiator/` and `internal/storage/**`.

## Conventions

- Ports are declared in the package that **uses** them (the module), not the one that implements them.
- Every adapter asserts conformance: `var _ module.AuditLog = (*audit)(nil)`.
- Constructors return the **port interface**: `func NewAudit(q db.Querier) module.AuditLog`.
- Adapters translate driver errors into `apperrors` types (`ErrNotFound`, `ErrDBRead`, `ErrDBWrite`, ...). The core never sees `pgx.ErrNoRows`.
- Inbound adapters map errors outward:
  - HTTP uses `errorMapper.toHuma` (`router/errors.go`).
  - Temporal maps business errors to non-retryable application errors.
  - Kafka: the generic `consumer.go` treats `ErrInvalidInput` as poison; the audit consumer must never drop a record (see `add-kafka-consumer`).
- Validation: Huma tags at the edge (shape), `validator` tags in models plus checks in the module (invariants).
- Convert at boundaries:
  - DTO ↔ model in the router;
  - sqlc row ↔ model in the repository (`auditToModel`, `toInsertParams`);
  - SDK/proto type ↔ model in the adapter.
- Never edit generated code: `internal/storage/repository/db`, `internal/module/mocks`, `pkg/dpsapi/gen`.

## Checklist before finishing

- [ ] `make generate` if SQL, proto or ports changed
- [ ] `make lint` (architecture rules) passes
- [ ] `make test` passes
- [ ] Benchmark added or updated if a use case, endpoint, handler, activity or adapter changed (see `add-benchmark`). Run `make bench-compare` if `bench/baseline.txt` exists.
