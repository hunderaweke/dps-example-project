// Package models holds the domain entities. It is the innermost layer and must
// not import any other internal package or infrastructure library.
package models

import "time"

// AuditRecord is one DPS domain event as the audit trail keeps it: append-only,
// one row per event, the payload stored as received. Tamper evidence comes from
// the WORM copy in object storage, not from this row.
type AuditRecord struct {
	// EventID is EventMetadata.event_id, set by the producer when it wrote its
	// outbox row; a retried publish carries the same ID, so it is the dedupe
	// key. When the metadata cannot be decoded it is
	// "kafka:<topic>/<partition>/<offset>", which a redelivery repeats too.
	EventID          string `validate:"required"`
	EventType        string `validate:"required"` // domain.entity.event.vN
	AggregateID      string
	AggregateVersion int64
	Producer         string
	ProducerImpl     string
	CorrelationID    string
	CausationID      string
	Actor            Actor
	Channel          string
	// OccurredAt is EventMetadata.occurred_at, or the Kafka record timestamp
	// when the metadata cannot be decoded.
	OccurredAt time.Time `validate:"required"`
	// MetadataValid is false when EventMetadata could not be decoded; the
	// record is still kept so nothing published is lost.
	MetadataValid bool

	// Where the record was read from.
	Topic     string `validate:"required"`
	Partition int32
	Offset    int64

	// Payload is the record value exactly as received (protobuf in Schema
	// Registry wire format). It may be empty; the record is still kept.
	Payload []byte

	// RecordedAt is set by the store when the row is written.
	RecordedAt time.Time
}

// Actor is who caused the event, from EventMetadata.actor.
type Actor struct {
	Type string
	ID   string
	Role string
}

// Page is a generic pagination request used across modules.
type Page struct {
	Limit  int32
	Offset int32
}
