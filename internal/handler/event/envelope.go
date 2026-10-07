package event

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/hunderaweke/dps-audit-service/internal/const/events"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	eventsv1 "github.com/hunderaweke/dps-audit-service/pkg/dpsapi/gen/dps/events/v1"
)

// unknownEventType is stored when neither the header nor the metadata names the event.
const unknownEventType = "unknown"

var errNoMetadata = errors.New("field 1 (EventMetadata) not found")

// decodeRecord turns a Kafka record into an audit record. It never fails: a
// value whose EventMetadata cannot be read is still recorded, with
// MetadataValid false, an ID derived from its Kafka position and the record
// timestamp, so nothing published is lost. The payload is kept as received.
func decodeRecord(rec *kgo.Record) models.AuditRecord {
	out := models.AuditRecord{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Payload:   rec.Value,
	}
	if out.Payload == nil {
		out.Payload = []byte{}
	}
	headerType := header(rec, events.HeaderEventType)

	meta, err := decodeMetadata(rec.Value)
	if err != nil || meta.GetEventId() == "" {
		out.EventID = fmt.Sprintf("kafka:%s/%d/%d", rec.Topic, rec.Partition, rec.Offset)
		out.EventType = headerType
		if out.EventType == "" {
			out.EventType = meta.GetEventType() // nil-safe; set when only event_id was missing
		}
		if out.EventType == "" {
			out.EventType = unknownEventType
		}
		out.OccurredAt = rec.Timestamp
		return withTime(out)
	}

	out.MetadataValid = true
	out.EventID = meta.GetEventId()
	out.EventType = meta.GetEventType()
	if out.EventType == "" {
		out.EventType = headerType
	}
	if out.EventType == "" {
		out.EventType = unknownEventType
	}
	out.AggregateID = meta.GetAggregateId()
	out.AggregateVersion = meta.GetAggregateVersion()
	out.Producer = meta.GetProducer()
	out.ProducerImpl = enumName(meta.GetProducerImpl().String(), "PRODUCER_IMPL_")
	out.CorrelationID = meta.GetCorrelationId()
	out.CausationID = meta.GetCausationId()
	if a := meta.GetActor(); a != nil {
		out.Actor = models.Actor{Type: enumName(a.GetType().String(), "ACTOR_TYPE_"), ID: a.GetId(), Role: a.GetRole()}
	}
	out.OccurredAt = rec.Timestamp
	if ts := meta.GetOccurredAt(); ts != nil && ts.IsValid() && ts.GetSeconds() > 0 {
		out.OccurredAt = ts.AsTime()
	}
	return withTime(out)
}

// withTime guarantees OccurredAt is set, so the record always passes the
// core's validation; a record that failed it would block its partition.
func withTime(r models.AuditRecord) models.AuditRecord {
	if r.OccurredAt.IsZero() {
		r.OccurredAt = time.Now().UTC()
	}
	return r
}

// decodeMetadata reads field 1 of a DPS event message, which is always
// EventMetadata. The value may carry the Schema Registry wire-format prefix.
func decodeMetadata(value []byte) (*eventsv1.EventMetadata, error) {
	msg, err := stripSchemaRegistry(value)
	if err != nil {
		return nil, err
	}
	for len(msg) > 0 {
		num, typ, n := protowire.ConsumeTag(msg)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		msg = msg[n:]
		if num == 1 && typ == protowire.BytesType {
			b, n := protowire.ConsumeBytes(msg)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			meta := &eventsv1.EventMetadata{}
			if err := proto.Unmarshal(b, meta); err != nil {
				return nil, err
			}
			return meta, nil
		}
		n = protowire.ConsumeFieldValue(num, typ, msg)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		msg = msg[n:]
	}
	return nil, errNoMetadata
}

// stripSchemaRegistry removes the Schema Registry protobuf framing: magic byte
// 0, a 4-byte schema ID, then the message-index list (a varint count followed
// by that many varints; a lone 0 means the first message). A value that does
// not start with the magic byte is treated as plain protobuf.
func stripSchemaRegistry(value []byte) ([]byte, error) {
	if len(value) == 0 || value[0] != 0 {
		return value, nil
	}
	if len(value) < 6 {
		return nil, errors.New("schema registry prefix truncated")
	}
	_ = binary.BigEndian.Uint32(value[1:5]) // schema ID; the payload is stored as received
	rest := value[5:]
	count, n := protowire.ConsumeVarint(rest)
	if n < 0 {
		return nil, protowire.ParseError(n)
	}
	rest = rest[n:]
	for range count {
		_, n := protowire.ConsumeVarint(rest)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		rest = rest[n:]
	}
	return rest, nil
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// enumName turns PRODUCER_IMPL_REAL into "real"; the UNSPECIFIED value is "".
func enumName(name, prefix string) string {
	s := strings.ToLower(strings.TrimPrefix(name, prefix))
	if s == "unspecified" {
		return ""
	}
	return s
}
