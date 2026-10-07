// Package perf turns benchmarks into pass/fail performance budgets.
//
// Each package keeps one table of cases. BenchmarkX runs every case with
// b.Run for benchstat; TestXBudget runs the same cases with Enforce, which
// fails when a case is slower, allocates more or moves less than its budget.
package perf

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// EnvVar enables budget enforcement. Without it Enforce skips, so plain
// `go test` stays fast and is not flaky on a loaded machine.
const EnvVar = "PERF_BUDGETS"

// runs is how many times Enforce measures a case; the best run is checked,
// which absorbs scheduler and GC noise.
const runs = 3

// Budget holds the limits of one case. A zero field is not checked.
type Budget struct {
	MaxNsPerOp     time.Duration
	MaxAllocsPerOp int64
	// MinPerSec holds lower bounds for rates the benchmark reports with
	// b.ReportMetric, keyed by unit, e.g. "events/s".
	MinPerSec map[string]float64
}

// Enforce runs fn with testing.Benchmark and fails t when the best of runs
// measurements breaks b. It skips unless PERF_BUDGETS=1.
func Enforce(t *testing.T, name string, fn func(*testing.B), b Budget) {
	t.Helper()
	if os.Getenv(EnvVar) != "1" {
		t.Skipf("set %s=1 to enforce performance budgets", EnvVar)
	}
	best := measure(t, name, fn)
	for _, msg := range violations(name, best, b) {
		t.Error(msg)
	}
	t.Logf("%s: %v/op, %d allocs/op%s", name, time.Duration(best.ns), best.allocs, formatRates(best.rates))
}

type result struct {
	ns     int64
	allocs int64
	rates  map[string]float64
}

// measure keeps, across runs, the lowest ns/op and allocs/op and the highest
// of each reported rate.
func measure(t *testing.T, name string, fn func(*testing.B)) result {
	t.Helper()
	var best result
	for i := range runs {
		r := testing.Benchmark(fn)
		if r.N == 0 {
			t.Fatalf("%s: benchmark failed or was skipped", name)
		}
		if i == 0 || r.NsPerOp() < best.ns {
			best.ns = r.NsPerOp()
		}
		if i == 0 || r.AllocsPerOp() < best.allocs {
			best.allocs = r.AllocsPerOp()
		}
		if best.rates == nil {
			best.rates = map[string]float64{}
		}
		for unit, v := range r.Extra {
			if v > best.rates[unit] {
				best.rates[unit] = v
			}
		}
	}
	return best
}

func violations(name string, r result, b Budget) []string {
	var out []string
	if b.MaxNsPerOp > 0 && r.ns > b.MaxNsPerOp.Nanoseconds() {
		out = append(out, fmt.Sprintf("%s: %v/op exceeds budget %v/op", name, time.Duration(r.ns), b.MaxNsPerOp))
	}
	if b.MaxAllocsPerOp > 0 && r.allocs > b.MaxAllocsPerOp {
		out = append(out, fmt.Sprintf("%s: %d allocs/op exceeds budget %d allocs/op", name, r.allocs, b.MaxAllocsPerOp))
	}
	for unit, minimum := range b.MinPerSec {
		got, ok := r.rates[unit]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("%s: no %q metric reported", name, unit))
		case got < minimum:
			out = append(out, fmt.Sprintf("%s: %.0f %s is below budget %.0f %s", name, got, unit, minimum, unit))
		}
	}
	return out
}

func formatRates(rates map[string]float64) string {
	var sb strings.Builder
	for unit, v := range rates {
		fmt.Fprintf(&sb, ", %.0f %s", v, unit)
	}
	return sb.String()
}
