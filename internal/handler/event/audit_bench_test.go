package event

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.uber.org/zap"

	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/testutil/perf"
)

// benchAudit is a hand-written module.Audit fake so the benchmarks measure the
// consumer, not mock bookkeeping. delay stands in for a database round trip.
type benchAudit struct{ delay time.Duration }

func (a benchAudit) Record(_ context.Context, recs []models.AuditRecord) (int64, error) {
	if a.delay > 0 {
		time.Sleep(a.delay)
	}
	return int64(len(recs)), nil
}

func (benchAudit) Get(context.Context, string) (models.AuditRecord, error) {
	return models.AuditRecord{}, nil
}

func newBenchConsumer(audit benchAudit, batchSize, maxWorkers int) *AuditConsumer {
	cfg := AuditConsumerConfig{BatchSize: batchSize, MaxWorkers: maxWorkers}
	return NewAuditConsumer(nil, kotel.NewTracer(), audit, cfg, zap.NewNop())
}

// benchRecords builds n valid, Schema Registry framed records of one
// partition, each with its own event ID and offset.
func benchRecords(tb testing.TB, partition int32, n int) []*kgo.Record {
	tb.Helper()
	recs := make([]*kgo.Record, n)
	for i := range recs {
		meta := testMetadata()
		meta.EventId = fmt.Sprintf("evt-%d-%d", partition, i)
		r := kafkaRecord(srFrame(eventMessage(tb, meta)), meta.EventType)
		r.Partition, r.Offset = partition, int64(i)
		recs[i] = r
	}
	return recs
}

// benchFetches builds one poll's worth of records without a broker.
func benchFetches(tb testing.TB, partitions, perPartition int) kgo.Fetches {
	tb.Helper()
	parts := make([]kgo.FetchPartition, partitions)
	for p := range parts {
		parts[p] = kgo.FetchPartition{Partition: int32(p), Records: benchRecords(tb, int32(p), perPartition)}
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "ledger.events", Partitions: parts}}}}
}

func reportRecordsPerSec(b *testing.B, recordsPerOp int) {
	b.ReportMetric(float64(recordsPerOp)*float64(b.N)/b.Elapsed().Seconds(), "records/s")
}

// storePartitionCase measures decoding, batching and the trace span for one
// partition, with a store that costs nothing.
type storePartitionCase struct {
	name           string
	records, batch int
	budget         perf.Budget
}

// Budgets: Apple M1 Pro, 2026-10-07, from the benchstat median of 6 runs:
// 2x ns/op, half the rate, allocs/op +10%. See the add-benchmark skill.
var storePartitionCases = []storePartitionCase{
	{name: "records_100/batch_100", records: 100, batch: 100,
		budget: perf.Budget{MaxNsPerOp: 130 * time.Microsecond, MaxAllocsPerOp: 1450}},
	{name: "records_1000/batch_100", records: 1000, batch: 100,
		budget: perf.Budget{MaxNsPerOp: 1200 * time.Microsecond, MaxAllocsPerOp: 14450}},
	{name: "records_1000/batch_500", records: 1000, batch: 500,
		budget: perf.Budget{MaxNsPerOp: 1250 * time.Microsecond, MaxAllocsPerOp: 14330}},
	{name: "records_5000/batch_500", records: 5000, batch: 500,
		budget: perf.Budget{MaxNsPerOp: 7 * time.Millisecond, MaxAllocsPerOp: 71640}},
}

func (c storePartitionCase) run(b *testing.B) {
	ctx := context.Background()
	sut := newBenchConsumer(benchAudit{}, c.batch, 1)
	recs := benchRecords(b, 0, c.records)
	b.ReportAllocs()
	for b.Loop() {
		if err := sut.storePartition(ctx, recs); err != nil {
			b.Fatal(err)
		}
	}
	reportRecordsPerSec(b, c.records)
}

func BenchmarkStorePartition(b *testing.B) {
	for _, c := range storePartitionCases {
		b.Run(c.name, c.run)
	}
}

func TestStorePartitionBudget(t *testing.T) {
	for _, c := range storePartitionCases {
		t.Run(c.name, func(t *testing.T) { perf.Enforce(t, "StorePartition/"+c.name, c.run, c.budget) })
	}
}

// storeFetchesCase measures one poll fanned out over partitions. The store
// sleeps per batch like a database would, so more workers must finish sooner.
type storeFetchesCase struct {
	name    string
	workers int
	delay   time.Duration
	budget  perf.Budget
}

const (
	fetchPartitions   = 16
	fetchPerPartition = 300
)

// Budgets: Apple M1 Pro, 2026-10-07, from the benchstat median of 6 runs:
// 2x ns/op, half the rate, allocs/op +10%. See the add-benchmark skill.
// workers_10 at 6ms against workers_1 at 65ms also proves the fan-out works.
var storeFetchesCases = []storeFetchesCase{
	{name: "delay_1ms/workers_1", workers: 1, delay: time.Millisecond,
		budget: perf.Budget{MaxNsPerOp: 65 * time.Millisecond, MaxAllocsPerOp: 69000}},
	{name: "delay_1ms/workers_4", workers: 4, delay: time.Millisecond,
		budget: perf.Budget{MaxNsPerOp: 13500 * time.Microsecond, MaxAllocsPerOp: 69000}},
	{name: "delay_1ms/workers_10", workers: 10, delay: time.Millisecond,
		budget: perf.Budget{MaxNsPerOp: 6 * time.Millisecond, MaxAllocsPerOp: 69000}},
	{name: "delay_1ms/workers_16", workers: 16, delay: time.Millisecond,
		budget: perf.Budget{MaxNsPerOp: 4200 * time.Microsecond, MaxAllocsPerOp: 69000}},
	{name: "delay_0/workers_10", workers: 10,
		budget: perf.Budget{MaxNsPerOp: 2800 * time.Microsecond, MaxAllocsPerOp: 69000}},
}

func (c storeFetchesCase) run(b *testing.B) {
	ctx := context.Background()
	sut := newBenchConsumer(benchAudit{delay: c.delay}, 500, c.workers)
	fetches := benchFetches(b, fetchPartitions, fetchPerPartition)
	b.ReportAllocs()
	for b.Loop() {
		if err := sut.storeFetches(ctx, fetches); err != nil {
			b.Fatal(err)
		}
	}
	reportRecordsPerSec(b, fetchPartitions*fetchPerPartition)
}

func BenchmarkStoreFetches(b *testing.B) {
	for _, c := range storeFetchesCases {
		b.Run(c.name, c.run)
	}
}

func TestStoreFetchesBudget(t *testing.T) {
	for _, c := range storeFetchesCases {
		t.Run(c.name, func(t *testing.T) { perf.Enforce(t, "StoreFetches/"+c.name, c.run, c.budget) })
	}
}
