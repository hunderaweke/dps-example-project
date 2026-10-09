# example-service

Go 1.27 service template using **hexagonal architecture** (ports and adapters). Read `README.md` §2 for the full architecture and `docs/architecture.md` for runtime flows, deployment and the roadmap.

## Must know

- Core = `internal/module` (ports + use cases). Domain = `internal/const/models`. Adapters:
  - inbound: `internal/router`, `internal/handler/{event,workflow}`;
  - outbound: `internal/storage/*`.
- Composition root = `initiator/`, the only place that knows concrete types.
- Import rules are enforced by depguard: **run `make lint` after every change.**
- Never edit generated code:
  - `internal/storage/repository/db` (`make sqlc`)
  - `internal/module/mocks` (`make mocks`)
  - `pkg/*/gen` (`make proto`)
- Errors: adapters wrap driver errors into `internal/const/errors` types. The router maps them with `HTTPStatus`.
- Validation: Huma tags at the edge, `validator` tags and checks in the core.
- Comments (strict): write one only when the code cannot explain itself. Clear names come first.
  - Allowed: a non-obvious *why* (an invariant, a workaround, idempotency, determinism, a security or ordering constraint), a non-obvious contract, and directives (`//go:`, `//nolint`).
  - Forbidden: restating a name or signature ("NewX returns an X"), section dividers, narrating what the next lines do, repeating architecture rules (depguard and the skills own those), commented-out code, and change history.
  - Package and exported doc comments follow the same rule. Omit them when the name says it all.
- Benchmarks: a new or changed use case, endpoint, Kafka handler, activity or storage adapter ships with a `b.Loop` benchmark in the same change. The rules are in the `add-benchmark` skill.

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
make bench-compare        # benchstat vs bench/baseline.txt
```
