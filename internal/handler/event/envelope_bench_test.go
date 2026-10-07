package event

import "testing"

// BenchmarkDecodeRecord measures the per-record cost of the audit consumer's
// decode step: Schema Registry framing plus EventMetadata unmarshal.
func BenchmarkDecodeRecord(b *testing.B) {
	rec := kafkaRecord(srFrame(eventMessage(b, testMetadata())), "ledger.operation.completed.v1")
	b.ReportAllocs()
	for b.Loop() {
		if r := decodeRecord(rec); !r.MetadataValid {
			b.Fatal("decode failed")
		}
	}
}
