// Package repository holds persistence adapters. Each adapter implements an
// outbound port from internal/module and translates driver errors into
// internal/const/errors types so the core never sees pgx errors.
package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	apperrors "github.com/hunderaweke/dps-audit-service/internal/const/errors"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository/db"
)

type audit struct {
	q db.Querier
}

var _ module.AuditLog = (*audit)(nil)

// NewAudit accepts the sqlc Querier, which both *db.Queries (pool) and
// db.New(tx) satisfy, so it can be used inside transactions too.
func NewAudit(q db.Querier) module.AuditLog {
	return &audit{q: q}
}

// Append writes the batch in one statement; rows whose event_id is already
// stored are skipped, so the returned count can be lower than len(recs).
func (r *audit) Append(ctx context.Context, recs []models.AuditRecord) (int64, error) {
	if len(recs) == 0 {
		return 0, nil
	}
	n, err := r.q.InsertAuditRecords(ctx, toInsertParams(recs))
	if err != nil {
		return 0, apperrors.ErrDBWrite.Wrap(err, "append %d audit records", len(recs))
	}
	return n, nil
}

func (r *audit) Get(ctx context.Context, eventID string) (models.AuditRecord, error) {
	row, err := r.q.GetAuditRecord(ctx, eventID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.AuditRecord{}, apperrors.ErrNotFound.New("audit record %s not found", eventID)
		}
		return models.AuditRecord{}, apperrors.ErrDBRead.Wrap(err, "read audit record %s", eventID)
	}
	return auditToModel(row), nil
}

func toInsertParams(recs []models.AuditRecord) db.InsertAuditRecordsParams {
	n := len(recs)
	p := db.InsertAuditRecordsParams{
		EventIds: make([]string, n), EventTypes: make([]string, n), AggregateIds: make([]string, n),
		AggregateVersions: make([]int64, n), Producers: make([]string, n), ProducerImpls: make([]string, n),
		CorrelationIds: make([]string, n), CausationIds: make([]string, n), ActorTypes: make([]string, n),
		ActorIds: make([]string, n), ActorRoles: make([]string, n), Channels: make([]string, n),
		OccurredAts: make([]time.Time, n), MetadataValids: make([]bool, n), KafkaTopics: make([]string, n),
		KafkaPartitions: make([]int32, n), KafkaOffsets: make([]int64, n), Payloads: make([][]byte, n),
	}
	for i, r := range recs {
		p.EventIds[i] = r.EventID
		p.EventTypes[i] = r.EventType
		p.AggregateIds[i] = r.AggregateID
		p.AggregateVersions[i] = r.AggregateVersion
		p.Producers[i] = r.Producer
		p.ProducerImpls[i] = r.ProducerImpl
		p.CorrelationIds[i] = r.CorrelationID
		p.CausationIds[i] = r.CausationID
		p.ActorTypes[i] = r.Actor.Type
		p.ActorIds[i] = r.Actor.ID
		p.ActorRoles[i] = r.Actor.Role
		p.Channels[i] = r.Channel
		p.OccurredAts[i] = r.OccurredAt
		p.MetadataValids[i] = r.MetadataValid
		p.KafkaTopics[i] = r.Topic
		p.KafkaPartitions[i] = r.Partition
		p.KafkaOffsets[i] = r.Offset
		p.Payloads[i] = r.Payload
	}
	return p
}

func auditToModel(row db.AuditRecord) models.AuditRecord {
	return models.AuditRecord{
		EventID:          row.EventID,
		EventType:        row.EventType,
		AggregateID:      row.AggregateID,
		AggregateVersion: row.AggregateVersion,
		Producer:         row.Producer,
		ProducerImpl:     row.ProducerImpl,
		CorrelationID:    row.CorrelationID,
		CausationID:      row.CausationID,
		Actor:            models.Actor{Type: row.ActorType, ID: row.ActorID, Role: row.ActorRole},
		Channel:          row.Channel,
		OccurredAt:       row.OccurredAt,
		MetadataValid:    row.MetadataValid,
		Topic:            row.KafkaTopic,
		Partition:        row.KafkaPartition,
		Offset:           row.KafkaOffset,
		Payload:          row.Payload,
		RecordedAt:       row.RecordedAt,
	}
}
