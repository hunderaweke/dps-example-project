package event

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/hunderaweke/dps-audit-service/internal/const/events"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	commonv1 "github.com/hunderaweke/dps-audit-service/pkg/dpsapi/gen/dps/common/v1"
	eventsv1 "github.com/hunderaweke/dps-audit-service/pkg/dpsapi/gen/dps/events/v1"
)

var occurred = time.Date(2026, 10, 5, 9, 30, 0, 0, time.UTC)

func testMetadata() *eventsv1.EventMetadata {
	return &eventsv1.EventMetadata{
		EventId:          "evt-1",
		EventType:        "ledger.operation.completed.v1",
		AggregateId:      "op-42",
		AggregateVersion: 3,
		OccurredAt:       timestamppb.New(occurred),
		Producer:         "ledger",
		ProducerImpl:     eventsv1.ProducerImpl_PRODUCER_IMPL_MOCK,
		CorrelationId:    "corr-9",
		CausationId:      "evt-0",
		Actor:            &commonv1.Actor{Type: commonv1.ActorType_ACTOR_TYPE_CUSTOMER, Id: "cust-7", Role: ""},
	}
}

// eventMessage encodes a DPS event message: field 1 is the metadata, field 2
// stands in for the event body.
func eventMessage(t testing.TB, meta *eventsv1.EventMetadata) []byte {
	t.Helper()
	m, err := proto.Marshal(meta)
	require.NoError(t, err)
	b := protowire.AppendTag(nil, 2, protowire.BytesType) // body first: field order must not matter
	b = protowire.AppendBytes(b, []byte("body"))
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	return protowire.AppendBytes(b, m)
}

// srFrame adds the Schema Registry prefix: magic 0, schema ID, message indexes.
func srFrame(msg []byte, indexes ...uint64) []byte {
	b := []byte{0, 0, 0, 0, 42}
	if len(indexes) == 0 {
		b = append(b, 0)
	} else {
		b = protowire.AppendVarint(b, uint64(len(indexes)))
		for _, i := range indexes {
			b = protowire.AppendVarint(b, i)
		}
	}
	return append(b, msg...)
}

func kafkaRecord(value []byte, eventType string) *kgo.Record {
	r := &kgo.Record{Topic: "ledger.events", Partition: 2, Offset: 77, Value: value,
		Timestamp: time.Date(2026, 10, 5, 9, 31, 0, 0, time.UTC)}
	if eventType != "" {
		r.Headers = []kgo.RecordHeader{{Key: events.HeaderEventType, Value: []byte(eventType)}}
	}
	return r
}

func TestDecodeRecordValid(t *testing.T) {
	msg := eventMessage(t, testMetadata())
	for name, value := range map[string][]byte{
		"schema registry framing":           srFrame(msg),
		"schema registry with nested index": srFrame(msg, 1, 0),
		"plain protobuf":                    msg,
	} {
		t.Run(name, func(t *testing.T) {
			rec := kafkaRecord(value, "ledger.operation.completed.v1")
			got := decodeRecord(rec)
			assert.Equal(t, models.AuditRecord{
				EventID:          "evt-1",
				EventType:        "ledger.operation.completed.v1",
				AggregateID:      "op-42",
				AggregateVersion: 3,
				Producer:         "ledger",
				ProducerImpl:     "mock",
				CorrelationID:    "corr-9",
				CausationID:      "evt-0",
				Actor:            models.Actor{Type: "customer", ID: "cust-7"},
				OccurredAt:       occurred,
				MetadataValid:    true,
				Topic:            "ledger.events",
				Partition:        2,
				Offset:           77,
				Payload:          value,
			}, got)
		})
	}
}

func TestDecodeRecordMetadataTypeWins(t *testing.T) {
	got := decodeRecord(kafkaRecord(srFrame(eventMessage(t, testMetadata())), "something.else.v1"))
	assert.Equal(t, "ledger.operation.completed.v1", got.EventType)

	meta := testMetadata()
	meta.EventType = ""
	got = decodeRecord(kafkaRecord(srFrame(eventMessage(t, meta)), "ledger.operation.completed.v1"))
	assert.Equal(t, "ledger.operation.completed.v1", got.EventType, "header is the fallback")
}

func TestDecodeRecordUnspecifiedEnums(t *testing.T) {
	meta := testMetadata()
	meta.ProducerImpl = eventsv1.ProducerImpl_PRODUCER_IMPL_UNSPECIFIED
	meta.Actor = nil
	got := decodeRecord(kafkaRecord(srFrame(eventMessage(t, meta)), ""))
	assert.Empty(t, got.ProducerImpl)
	assert.Equal(t, models.Actor{}, got.Actor)
}

func TestDecodeRecordInvalidIsStillRecorded(t *testing.T) {
	noID := testMetadata()
	noID.EventId = ""
	cases := map[string]struct {
		value      []byte
		header     string
		wantType   string
		wantNilPay bool
	}{
		"garbage bytes":             {value: []byte("not protobuf at all"), header: "ledger.operation.completed.v1", wantType: "ledger.operation.completed.v1"},
		"truncated registry prefix": {value: []byte{0, 0, 1}, header: "ledger.operation.completed.v1", wantType: "ledger.operation.completed.v1"},
		"metadata without event id": {value: srFrame(eventMessage(t, noID)), header: "", wantType: "ledger.operation.completed.v1"},
		"no metadata field":         {value: srFrame(protowire.AppendVarint(protowire.AppendTag(nil, 3, protowire.VarintType), 9)), header: "", wantType: unknownEventType},
		"empty value, no header":    {value: nil, header: "", wantType: unknownEventType, wantNilPay: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := kafkaRecord(tc.value, tc.header)
			got := decodeRecord(rec)
			assert.False(t, got.MetadataValid)
			assert.Equal(t, "kafka:ledger.events/2/77", got.EventID)
			assert.Equal(t, tc.wantType, got.EventType)
			assert.Equal(t, rec.Timestamp, got.OccurredAt)
			assert.NotNil(t, got.Payload, "payload is never nil")
			if !tc.wantNilPay {
				assert.Equal(t, tc.value, got.Payload)
			}
		})
	}
}

func TestDecodeRecordAlwaysHasATime(t *testing.T) {
	rec := kafkaRecord([]byte("junk"), "")
	rec.Timestamp = time.Time{}
	assert.False(t, decodeRecord(rec).OccurredAt.IsZero())
}
