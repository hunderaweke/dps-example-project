---
name: add-grpc-client
description: Add a client for an external gRPC service to this Go service - proto in pkg/, buf generation, reusable SDK, outbound port, storage adapter mapping gRPC codes to app errors, config and wiring, plus an optional local stub. Use when the service needs to call another service over gRPC.
---

# Add a gRPC client

Reference files:
- `pkg/account/proto/account/v1/account.proto`, `buf.yaml`, `buf.gen.yaml`
- `pkg/account/grpc.go` (Dial with otelgrpc), `pkg/account/client.go` (SDK)
- `internal/storage/account/account.go` (port adapter)
- `tests/stubs/account/main.go` (local stub)

## Steps

1. **Proto**: `pkg/<svc>/proto/<svc>/v1/<svc>.proto` with `package <svc>.v1;` and `option go_package = "<module>/pkg/<svc>/gen/<svc>/v1;<svc>v1";`. If the contract lives in a shared repo, depend on it through buf instead of copying it.
2. **buf**: add `- path: pkg/<svc>/proto` to `buf.yaml` modules. In `buf.gen.yaml`, add plugin entries with `out: pkg/<svc>/gen` (or switch to per-module output). Run `make proto`, which lints too.
3. **SDK** in `pkg/<svc>/`:
   - `grpc.go`: `Dial(address, opts...)` using `grpc.NewClient` with `otelgrpc.NewClientHandler()`.
   - `client.go`: a `Client` with per-call timeouts that returns **its own plain types** (not proto messages) and raw gRPC errors.
   - It must not import `internal/...`. depguard enforces this.
4. **Port**: in `internal/module/<domain>.go`, add something like `type XClient interface { GetThing(ctx, id string) (models.Thing, error) }`. Add any new model to `models`. Run `make mocks`.
5. **Adapter**: `internal/storage/<svc>/<svc>.go` with `var _ module.XClient = (*client)(nil)`:
   - converts SDK types to models;
   - maps `status.Code(err)`: `NotFound` → `ErrNotFound`, `Unavailable`/`DeadlineExceeded` → `ErrUnavailable`, `InvalidArgument` → `ErrInvalidInput`, `PermissionDenied` → `ErrForbidden`, anything else → `ErrInternal`.
6. **Config**: add a `<Svc> struct { Address string; Timeout time.Duration }` to `config.Config`, plus `config.yaml` defaults and the README config table.
7. **Wiring**:
   - `initiator/platform.go`: add a `<Svc>Conn *grpc.ClientConn` field and a `needs` flag; dial it and register `Close` via `onClose`.
   - `initiator/module.go`: build the adapter when the conn is non-nil.
8. **Local dev** (optional): a stub server in `tests/stubs/<svc>/main.go` that implements `Unimplemented<Svc>ServiceServer`, plus a Dockerfile stage and a `compose.dev.yml` service, as for `account-stub`.
9. **Tests**: module tests mock the port. For the adapter, use `google.golang.org/grpc/test/bufconn` with a fake server to test code mapping.

10. **Benchmark** (see `add-benchmark`): benchmark the adapter's mapping over `bufconn` with a fake server in `internal/storage/<svc>/<svc>_bench_test.go`.

Verify: `make proto lint test`.
