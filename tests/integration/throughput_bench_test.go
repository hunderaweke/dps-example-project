package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hunderaweke/dps-audit-service/internal/testutil/perf"
)

// eventsPerOp is how many events one benchmark iteration produces and waits
// to see stored.
const eventsPerOp = 1000

// throughputCase runs the real worker against Redpanda and Postgres with one
// consumer configuration. An iteration is produce → poll → decode → insert →
// visible in audit_records.
type throughputCase struct {
	name       string
	batchSize  int
	maxWorkers int
	budget     perf.Budget
}

// Budgets: Apple M1 Pro, 2026-10-07, from the benchstat median of 6 runs:
// 2x ns/op, half the rate, allocs/op +10%. See the add-benchmark skill.
var throughputCases = []throughputCase{
	{name: "batch_100/workers_1", batchSize: 100, maxWorkers: 1,
		budget: perf.Budget{MaxNsPerOp: 95 * time.Millisecond, MaxAllocsPerOp: 75000, MinPerSec: map[string]float64{"events/s": 10500}}},
	{name: "batch_500/workers_1", batchSize: 500, maxWorkers: 1,
		budget: perf.Budget{MaxNsPerOp: 100 * time.Millisecond, MaxAllocsPerOp: 74300, MinPerSec: map[string]float64{"events/s": 10000}}},
	{name: "batch_500/workers_10", batchSize: 500, maxWorkers: 10,
		budget: perf.Budget{MaxNsPerOp: 80 * time.Millisecond, MaxAllocsPerOp: 73300, MinPerSec: map[string]float64{"events/s": 12700}}},
}

// throughputRig shares one Postgres + Redpanda across cases. next is the ID
// of the next event to produce, so every produced event is unique.
type throughputRig struct {
	h    *harness
	next int
}

func newThroughputRig(tb testing.TB) *throughputRig {
	return &throughputRig{h: setup(tb)}
}

func (r *throughputRig) run(c throughputCase) func(*testing.B) {
	return func(b *testing.B) {
		// Report failures on this benchmark, not on the harness's owner.
		h := *r.h
		h.t = b
		cfg := *h.cfg
		cfg.Audit.BatchSize = c.batchSize
		cfg.Audit.MaxWorkers = c.maxWorkers
		h.cfg = &cfg

		stop := h.startWorker()
		defer stop()
		// Warm up outside the timed loop: joining the group waits out
		// Redpanda's group_initial_rebalance_delay (3s), which would
		// otherwise be the whole measurement.
		r.produceAndWait(b, &h)
		// Fresh statistics make the wait query use the primary key; before
		// autovacuum runs, the planner seq-scans the growing table instead.
		if _, err := h.db.Exec(context.Background(), `ANALYZE audit_records`); err != nil {
			b.Fatal(err)
		}

		for b.Loop() {
			r.produceAndWait(b, &h)
		}
		b.ReportMetric(float64(eventsPerOp)*float64(b.N)/b.Elapsed().Seconds(), "events/s")
	}
}

// produceAndWait produces the next eventsPerOp events and polls until all
// of them are stored. It looks them up by primary key, so the wait costs the
// same however large audit_records has grown.
func (r *throughputRig) produceAndWait(b *testing.B, h *harness) {
	b.Helper()
	ids := make([]string, eventsPerOp)
	for i := range ids {
		ids[i] = fmt.Sprintf("evt-%04d", r.next+i)
	}
	h.produce(r.next, eventsPerOp)
	r.next += eventsPerOp

	deadline := time.Now().Add(60 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		if err := h.db.QueryRow(context.Background(),
			`SELECT count(*) FROM audit_records WHERE event_id = ANY($1)`, ids).Scan(&got); err != nil {
			b.Fatal(err)
		}
		if got == eventsPerOp {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Fatalf("stored %d of %d events", got, eventsPerOp)
}

func BenchmarkConsumerThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("requires docker")
	}
	rig := newThroughputRig(b)
	for _, c := range throughputCases {
		b.Run(c.name, rig.run(c))
	}
}

func TestConsumerThroughputBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("requires docker")
	}
	if os.Getenv(perf.EnvVar) != "1" {
		t.Skipf("set %s=1 to enforce performance budgets", perf.EnvVar)
	}
	rig := newThroughputRig(t)
	for _, c := range throughputCases {
		t.Run(c.name, func(t *testing.T) { perf.Enforce(t, "ConsumerThroughput/"+c.name, rig.run(c), c.budget) })
	}
}
