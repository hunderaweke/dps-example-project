-- name: InsertAuditRecords :execrows
-- Writes a batch in one statement. A record whose event_id is already stored
-- (redelivery or a re-published event) is skipped; the row count says how many
-- were new.
INSERT INTO audit_records (
    event_id, event_type, aggregate_id, aggregate_version, producer, producer_impl,
    correlation_id, causation_id, actor_type, actor_id, actor_role, channel,
    occurred_at, metadata_valid, kafka_topic, kafka_partition, kafka_offset, payload
)
SELECT
    unnest(@event_ids::text[]),
    unnest(@event_types::text[]),
    unnest(@aggregate_ids::text[]),
    unnest(@aggregate_versions::bigint[]),
    unnest(@producers::text[]),
    unnest(@producer_impls::text[]),
    unnest(@correlation_ids::text[]),
    unnest(@causation_ids::text[]),
    unnest(@actor_types::text[]),
    unnest(@actor_ids::text[]),
    unnest(@actor_roles::text[]),
    unnest(@channels::text[]),
    unnest(@occurred_ats::timestamptz[]),
    unnest(@metadata_valids::boolean[]),
    unnest(@kafka_topics::text[]),
    unnest(@kafka_partitions::integer[]),
    unnest(@kafka_offsets::bigint[]),
    unnest(@payloads::bytea[])
ON CONFLICT (event_id) DO NOTHING;

-- name: GetAuditRecord :one
SELECT * FROM audit_records WHERE event_id = $1;
