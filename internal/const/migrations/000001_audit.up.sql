-- One row per DPS domain event, append-only. event_id is the producer's
-- EventMetadata.event_id, so a redelivered or re-published event is a no-op
-- (INSERT ... ON CONFLICT DO NOTHING). On YugabyteDB the primary key is
-- hash-sharded, which spreads writes across tablets.
CREATE TABLE IF NOT EXISTS audit_records (
    event_id          TEXT        PRIMARY KEY,
    event_type        TEXT        NOT NULL,
    aggregate_id      TEXT        NOT NULL DEFAULT '',
    aggregate_version BIGINT      NOT NULL DEFAULT 0,
    producer          TEXT        NOT NULL DEFAULT '',
    producer_impl     TEXT        NOT NULL DEFAULT '',
    correlation_id    TEXT        NOT NULL DEFAULT '',
    causation_id      TEXT        NOT NULL DEFAULT '',
    actor_type        TEXT        NOT NULL DEFAULT '',
    actor_id          TEXT        NOT NULL DEFAULT '',
    actor_role        TEXT        NOT NULL DEFAULT '',
    channel           TEXT        NOT NULL DEFAULT '',
    occurred_at       TIMESTAMPTZ NOT NULL,
    metadata_valid    BOOLEAN     NOT NULL,
    kafka_topic       TEXT        NOT NULL,
    kafka_partition   INTEGER     NOT NULL,
    kafka_offset      BIGINT      NOT NULL,
    payload           BYTEA       NOT NULL,
    recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Metadata search served by the database. Actor and event-type search are
-- served by OpenSearch.
CREATE INDEX IF NOT EXISTS idx_audit_records_aggregate ON audit_records (aggregate_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_records_correlation ON audit_records (correlation_id, occurred_at DESC);
-- Kafka position, for reconciling with the WORM segments and replay checks.
CREATE INDEX IF NOT EXISTS idx_audit_records_position ON audit_records (kafka_topic, kafka_partition, kafka_offset);

-- Append-only: UPDATE and DELETE are refused even for the table owner. The
-- service's own role is additionally granted only INSERT and SELECT (ops).
CREATE OR REPLACE FUNCTION audit_records_refuse_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit_records is append-only';
END
$$;

CREATE TRIGGER audit_records_append_only
    BEFORE UPDATE OR DELETE ON audit_records
    FOR EACH ROW EXECUTE FUNCTION audit_records_refuse_change();
