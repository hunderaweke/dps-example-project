---
name: add-benchmark
description: Benchmark and profile code in this Go service - b.Loop micro-benchmarks per hexagonal layer, benchstat baseline comparison, pprof CPU/heap profiling (benchmarks and live admin port), and k6 load tests with thresholds. Use when asked about performance, latency, allocations, regressions, profiling or load testing.
---

# Benchmarks, profiling and load tests

Reference files:
- `internal/module/example_bench_test.go` (core, in-memory fakes)
- `internal/router/example_bench_test.go` (HTTP adapter)
- `internal/storage/{cache,repository}/example_bench_test.go` (adapters via testcontainers)
- `tests/load/example.js` (k6)

## Writing a micro-benchmark

```go
func BenchmarkThing(b *testing.B) {
    // setup here is excluded from timing automatically with b.Loop
    b.ReportAllocs()
    for b.Loop() {
        if _, err := sut.Do(ctx, input); err != nil { b.Fatal(err) }
    }
}
```

Rules:
- Use `for b.Loop()` (Go 1.24+). Do not use `b.N` loops or manual `b.ResetTimer()`.
- **Core benchmarks use hand-written in-memory fakes**, not mockery mocks: testify mocks add reflection and locking that dwarf the logic.
- **Router benchmarks** use `newServer(stub)` with a stub module embedding the port interface. They measure routing, Huma validation and JSON.
- **Adapter benchmarks** need Docker. Guard them with `if testing.Short() { b.Skip(...) }`, start the container with testcontainers, and clean up with `testcontainers.CleanupContainer(b, ctr)`.
- Use `b.Run("case", ...)` for variants, such as `cache_hit` and `cache_miss`.
- Always check errors inside the loop. A benchmark of a failing path is meaningless.

## Comparing (benchstat)

1. On the base branch: `make bench-baseline`, then commit `bench/baseline.txt`.
2. After changes: `make bench-compare`.
3. Only trust deltas with `p < 0.05`. `~` means no significant change. Keep `BENCH_COUNT` ≥ 6, and narrow the scope with `BENCH_PKGS=./internal/router`.

## Profiling

- Benchmark: `make bench-profile PKG=./internal/<pkg>` opens the CPU profile. For allocations: `go tool pprof -http=:0 bench/pkg.test bench/mem.out`.
- Running service: set `APP_SERVER__PPROF_PORT=6060` (it binds to 127.0.0.1). Then use `go tool pprof -http=:0 http://127.0.0.1:6060/debug/pprof/profile?seconds=30` (CPU) or `/heap`, and `/trace?seconds=5` with `go tool trace`.

## Load testing (k6)

- Edit `tests/load/example.js`. Give each request `tags: { name: '<endpoint>' }` and add a threshold such as `'http_req_duration{name:<endpoint>}': ['p(95)<X']`.
- Run against the stack with `make up && make load-test` (BASE_URL is overridable).
- k6 exits non-zero when a threshold fails, so it can gate CI. Correlate slow requests in Jaeger (http://localhost:16686).

Report results as benchstat tables or k6 summaries. Never quote a single run.
