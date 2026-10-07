---
name: add-benchmark
description: Benchmark and profile code in this Go service - b.Loop micro-benchmarks per hexagonal layer, benchstat baseline comparison, pprof CPU/heap profiling (benchmarks and live admin port), and k6 load tests with thresholds. Use when asked about performance, latency, allocations, regressions, profiling or load testing, AND proactively whenever you add or change a use case, endpoint, Kafka handler, Temporal activity or storage adapter (see "When a benchmark is required").
---

# Benchmarks, profiling and load tests

Reference files:
- `internal/testutil/perf` (budgets: `perf.Budget` and `perf.Enforce`, see "Budgets")
- `internal/module/audit_bench_test.go` (core: `BenchmarkRecord` with the hand-written fake `nopLog`)
- `internal/handler/event/audit_bench_test.go` (Kafka handler: `BenchmarkStorePartition`, `BenchmarkStoreFetches` over a built `kgo.Fetches`, fake `benchAudit` with a simulated store delay)
- `internal/handler/event/envelope_bench_test.go` (`BenchmarkDecodeRecord`: valid, invalid and 64 KiB payloads)
- `internal/storage/repository/audit_test.go` (adapter: `BenchmarkAuditRepository` over testcontainers Postgres, skipped with `-short`: append batches, duplicates, parallel appends, get)
- `tests/integration/throughput_bench_test.go` (end to end: `BenchmarkConsumerThroughput`, Redpanda to Postgres events/s with the real worker; `make bench-integration`)
- `tests/storagebench` (storage design comparison: YSQL write designs, S3 Object Lock segments, OpenSearch; `make bench-storage SB_PROFILE=quick|full`, results in `bench/storage/RESULTS.md`)

There is **no router or Temporal activity benchmark yet**, because none of those exist; the rules below say how to write them.

## When a benchmark is required

Add or update a benchmark in the **same change**, without being asked, when you:

| You change... | Add a benchmark in |
|---|---|
| a use case in `internal/module` (new method or new logic) | `internal/module/<domain>_bench_test.go`, in-memory fakes |
| an HTTP operation or its DTO | `internal/router/<domain>_bench_test.go`, plus a k6 request with a threshold if the endpoint is user-facing |
| a Kafka handler in `internal/handler/event` | `internal/handler/event/<domain>_bench_test.go` |
| a Temporal activity | `internal/handler/workflow/<domain>_bench_test.go` (call the activity directly, not via the test environment) |
| a storage adapter or a hot SQL query | `internal/storage/<kind>/<domain>_bench_test.go`, or next to the adapter's integration test as `BenchmarkAuditRepository` is (testcontainers, skipped with `-short`) |
| code you claim is faster or allocates less | a benchmark that shows it, and a benchstat comparison |

Not required for: wiring in `initiator/`, config, generated code, workflow functions (deterministic orchestration only), or pure renames.

When `bench/baseline.txt` exists (it does not yet; create it with `make bench-baseline` on the base branch), run `make bench-compare BENCH_PKGS=<touched packages>` before finishing and report any significant regression (`p < 0.05`). Otherwise, smoke-run with `go test -run='^$' -bench=. -benchtime=100x <pkgs>` to prove the benchmarks pass.

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
- **Core benchmarks use hand-written in-memory fakes** (like `nopLog`), not mockery mocks: testify mocks add reflection and locking that dwarf the logic.
- **Router benchmarks** register the route on a Huma test API (`humatest.New`) with a stub module embedding the port interface. They measure routing, Huma validation and JSON.
- **Adapter benchmarks** need Docker. Guard them with `if testing.Short() { b.Skip(...) }`, start the container with testcontainers, and clean up with `testcontainers.CleanupContainer(b, ctr)`. Benchmark realistic batch sizes (audit writes batches, not single rows).
- Use `b.Run("case", ...)` for variants, such as `batch_1` and `batch_100`, or `new` and `duplicate`.
- Always check errors inside the loop. A benchmark of a failing path is meaningless.

## Budgets (benchmarks that fail)

Every benchmark is a row in a case table with a `perf.Budget`. One table drives both the benchmark and its gate:

```go
type recordCase struct {
    name   string
    batch  int
    budget perf.Budget
}

var recordCases = []recordCase{
    {name: "batch_100", batch: 100, budget: perf.Budget{MaxNsPerOp: 40 * time.Microsecond, MaxAllocsPerOp: 2}},
}

func (c recordCase) run(b *testing.B) { /* setup, b.ReportAllocs(), for b.Loop() {...} */ }

func BenchmarkRecord(b *testing.B) {
    for _, c := range recordCases { b.Run(c.name, c.run) }
}

func TestRecordBudget(t *testing.T) {
    for _, c := range recordCases {
        t.Run(c.name, func(t *testing.T) { perf.Enforce(t, "Record/"+c.name, c.run, c.budget) })
    }
}
```

- A budget can cap `MaxNsPerOp` and `MaxAllocsPerOp`, and set `MinPerSec` floors for rates reported with `b.ReportMetric` (`records/s`, `rows/s`, `events/s`). A zero field is not checked.
- `perf.Enforce` skips unless `PERF_BUDGETS=1`, so `make test` is unaffected. It runs the case 3 times and checks the best run.
- Name the budget test `Test<Name>Budget`: `make bench-check` (no docker) and `make bench-check-all` (docker) select them with `-run=Budget`.
- **Setting a budget:** run the case with `-count=6`, take the benchstat median, then set ns/op to about 2× the median, a rate floor to about half, and allocs/op to the median plus 10% (allocations are deterministic, so a tight cap catches regressions). Note the machine and date above the table.
- A broken budget is a regression to fix, not a number to raise. Raise it only when the slowdown is intended, and say why in the change.

## Comparing (benchstat)

1. On the base branch: `make bench-baseline`, then commit `bench/baseline.txt`.
2. After changes: `make bench-compare`.
3. Only trust deltas with `p < 0.05`. `~` means no significant change. Keep `BENCH_COUNT` ≥ 6, and narrow the scope with `BENCH_PKGS=./internal/module`.

## Profiling

- Benchmark: `make bench-profile PKG=./internal/<pkg>` opens the CPU profile. For allocations: `go tool pprof -http=:0 bench/pkg.test bench/mem.out`.
- Running service: set `APP_SERVER__PPROF_PORT=6060` (it binds to 127.0.0.1). Then use `go tool pprof -http=:0 http://127.0.0.1:6060/debug/pprof/profile?seconds=30` (CPU) or `/heap`, and `/trace?seconds=5` with `go tool trace`.

## Load testing (k6)

- There is no k6 script or `make` target in the repo yet: the service has no business endpoint. When the read API lands, add a script under `tests/` and a `make` target for it.
- Give each request `tags: { name: '<endpoint>' }` and add a threshold such as `'http_req_duration{name:<endpoint>}': ['p(95)<X']`. Run it against `make up` with an overridable `BASE_URL`.
- k6 exits non-zero when a threshold fails, so it can gate CI. Correlate slow requests in Jaeger (http://localhost:16686).
- Consumer throughput is measured by the adapter benchmarks and `make bench-storage`, not by k6.

Report results as benchstat tables or k6 summaries. Never quote a single run.
