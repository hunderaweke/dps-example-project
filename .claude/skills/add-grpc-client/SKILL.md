---
name: add-grpc-client
description: Add a client for an external gRPC service to this Go service - proto in pkg/, buf generation, reusable SDK, outbound port, storage adapter mapping gRPC codes to app errors, config and wiring, plus an optional local stub. Use when the service needs to call another service over gRPC.
---

# Add a gRPC client

There is **no outbound gRPC client in the repo yet**. What exists for contracts:
- `buf.gen.yaml`: generates from the shared **dps-contracts** repo (git input, tag `v0.1.0`, `paths: dps/common/v1, dps/events/v1`) into `pkg/dpsapi/gen` with `protoc-gen-go`. There is no local `buf.yaml` and no `.proto` in this repo.
- `make proto` regenerates `pkg/dpsapi/gen`. Never edit it.

The steps below describe the pattern generically.

## Steps

1. **Contract**: DPS contracts live in dps-contracts, never copied here. Add the service's package path (for example `dps/<svc>/v1`) to `paths` in `buf.gen.yaml`, bumping `tag` if the contract is newer. Generated code lands in `pkg/dpsapi/gen/dps/<svc>/v1`.
2. **Generation**: add the `protoc-gen-go-grpc` plugin to `buf.gen.yaml` (`out: pkg/dpsapi/gen`, `opt: paths=source_relative`) and pin it as a `go tool`. Run `make proto`.
3. **SDK** in `pkg/<svc>/` (optional, if other services could reuse it):
   - `grpc.go`: `Dial(address, opts...)` using `grpc.NewClient` with `otelgrpc.NewClientHandler()`.
   - `client.go`: a `Client` with per-call timeouts that returns **its own plain types** (not proto messages) and raw gRPC errors.
   - It must not import `internal/...`. depguard enforces this.
4. **Port**: in `internal/module/<domain>.go`, add something like `type XClient interface { GetThing(ctx, id string) (models.Thing, error) }`. Add any new model to `models`. Run `make mocks`.
5. **Adapter**: `internal/storage/<svc>/<svc>.go` with `var _ module.XClient = (*client)(nil)`:
   - converts SDK or proto types to models;
   - maps `status.Code(err)`: `NotFound` → `ErrNotFound`, `Unavailable`/`DeadlineExceeded` → `ErrUnavailable`, `InvalidArgument` → `ErrInvalidInput`, `PermissionDenied` → `ErrForbidden`, anything else → `ErrInternal`.
6. **Config**: add a `<Svc> struct { Address string; Timeout time.Duration }` to `config.Config`, plus `config.yaml` defaults and the README config table.
7. **Wiring**:
   - `initiator/platform.go`: add a `<Svc>Conn *grpc.ClientConn` field and a `needs` flag; dial it and register `Close` via `onClose`.
   - `initiator/module.go`: build the adapter when the conn is non-nil.
8. **Local dev** (optional): a stub server in `tests/stubs/<svc>/main.go` that embeds `Unimplemented<Svc>ServiceServer`, plus a Dockerfile stage and a `compose.dev.yml` service.
9. **Tests**: module tests mock the port. For the adapter, use `google.golang.org/grpc/test/bufconn` with a fake server to test code mapping.

10. **Benchmark** (see `add-benchmark`): benchmark the adapter's mapping over `bufconn` with a fake server in `internal/storage/<svc>/<svc>_bench_test.go`.

Verify: `make proto lint test`.
