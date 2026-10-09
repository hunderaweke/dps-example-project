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
| is an entity, value object or event | `internal/const/models/` | `models/example.go` |
| is an error category | `internal/const/errors/errors.go` (reuse existing types first) | |
| is business logic / a use case | `internal/module/<domain>.go` | `module/example.go` |
| is an interface the core needs (DB, cache, broker, external API) | **outbound port** in `internal/module/<domain>.go` | `ExampleRepository` |
| reads/writes Postgres | `internal/storage/repository/<domain>.go` via sqlc `db.Querier` | `repository/example.go` |
| reads/writes Mongo | `internal/storage/repository/<name>.go` | `repository/audit.go` |
| touches Valkey | `internal/storage/cache/<domain>.go` | `cache/example.go` |
| produces Kafka records | `internal/storage/publisher/<domain>.go` | `publisher/example.go` |
| wraps an external gRPC SDK as a port | `internal/storage/<service>/` | `storage/account/account.go` |
| is a reusable client SDK with no internal deps | `pkg/<service>/` | `pkg/account/` |
| handles HTTP | `internal/router/<domain>.go` + DTOs in `internal/const/dto/` | `router/example.go` |
| consumes Kafka | `internal/handler/event/<domain>.go` | `handler/event/example.go` |
| is a Temporal workflow/activity | `internal/handler/workflow/<domain>.go` | `handler/workflow/example.go` |
| connects to infrastructure (pool, client, ping) | `internal/const/{database,cache,messaging,workflow}/` | `database/postgres/postgres.go` |
| wires things together | `initiator/` (`platform.go`, `module.go`, `handler.go`, `initiator.go`) | |

## Import rules (hard)

- `internal/const/models` must import **no** internal packages and no infrastructure libraries.
- `internal/module` (core) must **not** import any of these:
  - gin, huma, pgx, mongo-driver, go-redis, franz-go, go.temporal.io, grpc;
  - `internal/storage`, `internal/router`, `internal/handler`, `pkg/`, `initiator`.

  It may use `models`, `errors`, `validator`, `zap` and `uuid`.
- `internal/router` and `internal/handler/**` must **not** import `internal/storage`. They call the module through its port.
- `pkg/**` must **not** import `internal/**`.
- Only `cmd/*` imports `initiator`. The one exception is `tests/e2e`, which uses `initiator.BuildAPI`.
- `internal/const/dto` must **not** import `internal/storage`, `internal/const/{database,cache,messaging,workflow}` or any infrastructure driver.
- `internal/const/{database,cache,messaging,workflow}` may be imported only by `initiator/` and `internal/storage/**`.

## Conventions

- Ports are declared in the package that **uses** them (the module), not the one that implements them.
- Every adapter asserts conformance: `var _ module.ExampleRepository = (*example)(nil)`.
- Constructors return the **port interface**: `func NewExample(...) module.ExampleRepository`.
- Adapters translate driver errors into `apperrors` types (`ErrNotFound`, `ErrDBRead`, ...). The core never sees `pgx.ErrNoRows`.
- Inbound adapters map errors outward:
  - HTTP uses `errorMapper.toHuma`.
  - Temporal uses `toTemporalErr`.
  - Kafka treats `ErrInvalidInput` as poison.
- Validation: Huma tags at the edge (shape), `validator` tags in models plus checks in the module (invariants).
- Convert at boundaries:
  - DTO ↔ model in the router;
  - sqlc row ↔ model in the repository;
  - SDK type ↔ model in the storage adapter.
- Never edit generated code: `internal/storage/repository/db`, `internal/module/mocks`, `pkg/*/gen`.
- Comments: write one only for a non-obvious *why* or contract, or for a directive. Never restate names, signatures, layer rules or what the code plainly does. The full rule is in `CLAUDE.md`.

## Checklist before finishing

- [ ] `make generate` if SQL, proto or ports changed
- [ ] `make lint` (architecture rules) passes
- [ ] Every new comment explains something the code cannot
- [ ] `make test` passes
- [ ] Benchmark added or updated if a use case, endpoint, handler, activity or adapter changed (see `add-benchmark`). Run `make bench-compare` if `bench/baseline.txt` exists.
