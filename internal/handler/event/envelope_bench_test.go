package event

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/hunderaweke/dps-audit-service/internal/testutil/perf"
)

// decodeCase measures the per-record cost of the audit consumer's decode
// step: Schema Registry framing plus EventMetadata unmarshal.
type decodeCase struct {
	name      string
	record    func(testing.TB) *kgo.Record
	wantValid bool
	budget    perf.Budget
}

// Budgets: Apple M1 Pro, 2026-10-07, from the benchstat median of 6 runs:
// 2x ns/op, half the rate, allocs/op +10%. See the add-benchmark skill.
var decodeCases = []decodeCase{
	{name: "valid", wantValid: true,
		budget: perf.Budget{MaxNsPerOp: 1100 * time.Nanosecond, MaxAllocsPerOp: 15},
		record: func(tb testing.TB) *kgo.Record {
			return kafkaRecord(srFrame(eventMessage(tb, testMetadata())), "ledger.operation.completed.v1")
		}},
	{name: "invalid_payload",
		budget: perf.Budget{MaxNsPerOp: 300 * time.Nanosecond, MaxAllocsPerOp: 4},
		record: func(testing.TB) *kgo.Record {
			return kafkaRecord([]byte("not a dps event"), "ledger.operation.completed.v1")
		}},
	{name: "large_payload_64KiB", wantValid: true,
		budget: perf.Budget{MaxNsPerOp: 1100 * time.Nanosecond, MaxAllocsPerOp: 15},
		record: func(tb testing.TB) *kgo.Record {
			msg := eventMessage(tb, testMetadata())
			msg = protowire.AppendBytes(protowire.AppendTag(msg, 2, protowire.BytesType), make([]byte, 64<<10))
			return kafkaRecord(srFrame(msg), "ledger.operation.completed.v1")
		}},
}

func (c decodeCase) run(b *testing.B) {
	rec := c.record(b)
	b.ReportAllocs()
	for b.Loop() {
		if r := decodeRecord(rec); r.MetadataValid != c.wantValid {
			b.Fatalf("MetadataValid = %v, want %v", r.MetadataValid, c.wantValid)
		}
	}
}

func BenchmarkDecodeRecord(b *testing.B) {
	for _, c := range decodeCases {
		b.Run(c.name, c.run)
	}
}

func TestDecodeRecordBudget(t *testing.T) {
	for _, c := range decodeCases {
		t.Run(c.name, func(t *testing.T) { perf.Enforce(t, "DecodeRecord/"+c.name, c.run, c.budget) })
	}
}
