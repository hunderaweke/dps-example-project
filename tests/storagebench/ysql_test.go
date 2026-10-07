package storagebench_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaTables = `
DROP TABLE IF EXISTS audit_records;
DROP TABLE IF EXISTS chain_heads;
CREATE TABLE audit_records (
  chain_partition text        NOT NULL,
  sequence_no     bigint      NOT NULL,
  event_id        text        NOT NULL,
  event_type      text        NOT NULL,
  domain          text        NOT NULL,
  domain_bucket   smallint    NOT NULL,
  aggregate_id    text        NOT NULL,
  actor_id        text        NOT NULL,
  correlation_id  text        NOT NULL,
  channel         smallint    NOT NULL,
  occurred_at     timestamptz NOT NULL,
  day             date        NOT NULL,
  recorded_at     timestamptz NOT NULL DEFAULT now(),
  kafka_topic     text        NOT NULL,
  kafka_partition int         NOT NULL,
  kafka_offset    bigint      NOT NULL,
  payload_enc     bytea       NOT NULL,
  key_id          text        NOT NULL,
  payload_sha256  bytea       NOT NULL,
  prev_hash       bytea       NOT NULL,
  chain_hash      bytea       NOT NULL,
  PRIMARY KEY ((chain_partition) HASH, sequence_no ASC)
) SPLIT INTO 12 TABLETS;
CREATE UNIQUE INDEX audit_records_event_id ON audit_records (event_id HASH);
CREATE INDEX audit_by_aggregate ON audit_records (aggregate_id HASH, occurred_at DESC);
CREATE INDEX audit_by_correlation ON audit_records (correlation_id HASH, occurred_at DESC);
CREATE TABLE chain_heads (
  chain_partition text PRIMARY KEY,
  sequence_no     bigint      NOT NULL,
  chain_hash      bytea       NOT NULL,
  kafka_offset    bigint      NOT NULL,
  updated_at      timestamptz NOT NULL DEFAULT now()
);
`

// schemaSearchIndexes are the two indexes V3 leaves to OpenSearch.
const schemaSearchIndexes = `
CREATE INDEX audit_by_actor ON audit_records ((actor_id, day) HASH, occurred_at DESC);
CREATE INDEX audit_by_domain ON audit_records ((domain, day, domain_bucket) HASH, occurred_at DESC);
`

const schemaAppendOnly = `
CREATE OR REPLACE FUNCTION audit_refuse_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'audit_records is append-only';
END $$;
CREATE TRIGGER audit_records_append_only BEFORE UPDATE OR DELETE ON audit_records
  FOR EACH ROW EXECUTE FUNCTION audit_refuse_change();
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'audit_writer') THEN
    CREATE ROLE audit_writer LOGIN PASSWORD 'writer';
  END IF;
END $$;
REVOKE ALL ON audit_records, chain_heads FROM audit_writer;
GRANT SELECT, INSERT ON audit_records TO audit_writer;
GRANT SELECT, INSERT, UPDATE ON chain_heads TO audit_writer;
`

// truncateTrigger is tried separately: YSQL may not support statement-level
// TRUNCATE triggers. The writer role cannot TRUNCATE either way (no privilege).
const truncateTrigger = `CREATE TRIGGER audit_records_no_truncate BEFORE TRUNCATE ON audit_records
  FOR EACH STATEMENT EXECUTE FUNCTION audit_refuse_change();`

func resetSchema(ctx context.Context, admin *pgxpool.Pool, searchIndexes bool) (truncateTriggerOK bool, err error) {
	if _, err = admin.Exec(ctx, schemaTables); err != nil {
		return false, fmt.Errorf("tables: %w", err)
	}
	if searchIndexes {
		if _, err = admin.Exec(ctx, schemaSearchIndexes); err != nil {
			return false, fmt.Errorf("search indexes: %w", err)
		}
	}
	if _, err = admin.Exec(ctx, schemaAppendOnly); err != nil {
		return false, fmt.Errorf("append-only: %w", err)
	}
	_, terr := admin.Exec(ctx, truncateTrigger)
	return terr == nil, nil
}

func loadHeads(ctx context.Context, db *pgxpool.Pool) (map[string]*chainState, error) {
	rows, err := db.Query(ctx, `SELECT chain_partition, sequence_no, chain_hash, kafka_offset FROM chain_heads`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	heads := map[string]*chainState{}
	for rows.Next() {
		var c string
		s := &chainState{}
		if err := rows.Scan(&c, &s.seq, &s.hash, &s.offset); err != nil {
			return nil, err
		}
		heads[c] = s
	}
	return heads, rows.Err()
}

const insertRecords = `
INSERT INTO audit_records (chain_partition, sequence_no, event_id, event_type, domain, domain_bucket,
  aggregate_id, actor_id, correlation_id, channel, occurred_at, day, kafka_topic, kafka_partition,
  kafka_offset, payload_enc, key_id, payload_sha256, prev_hash, chain_hash)
SELECT * FROM unnest($1::text[], $2::int8[], $3::text[], $4::text[], $5::text[], $6::int2[],
  $7::text[], $8::text[], $9::text[], $10::int2[], $11::timestamptz[], $12::date[], $13::text[], $14::int4[],
  $15::int8[], $16::bytea[], $17::text[], $18::bytea[], $19::bytea[], $20::bytea[])`

const upsertHeads = `
INSERT INTO chain_heads (chain_partition, sequence_no, chain_hash, kafka_offset)
SELECT * FROM unnest($1::text[], $2::int8[], $3::bytea[], $4::int8[])
ON CONFLICT (chain_partition) DO UPDATE SET sequence_no = excluded.sequence_no,
  chain_hash = excluded.chain_hash, kafka_offset = excluded.kafka_offset, updated_at = now()`

// appendBatch writes linked records and the new chain heads in one transaction.
func appendBatch(ctx context.Context, db *pgxpool.Pool, recs []*record, heads map[string]*chainState) error {
	n := len(recs)
	a := struct {
		chain, eid, etype, dom, agg, actor, corr, topic, key []string
		seq, off                                             []int64
		bucket, ch                                           []int16
		at, day                                              []time.Time
		part                                                 []int32
		enc, ph, prev, hash                                  [][]byte
	}{}
	for _, r := range recs {
		a.chain = append(a.chain, r.Chain)
		a.seq = append(a.seq, r.Seq)
		a.eid = append(a.eid, r.EventID)
		a.etype = append(a.etype, r.EventType)
		a.dom = append(a.dom, r.Domain)
		a.bucket = append(a.bucket, r.DomainBucket)
		a.agg = append(a.agg, r.AggregateID)
		a.actor = append(a.actor, r.ActorID)
		a.corr = append(a.corr, r.CorrelationID)
		a.ch = append(a.ch, r.Channel)
		a.at = append(a.at, r.OccurredAt)
		a.day = append(a.day, r.OccurredAt.Truncate(24*time.Hour))
		a.topic = append(a.topic, r.Topic)
		a.part = append(a.part, r.Partition)
		a.off = append(a.off, r.Offset)
		a.enc = append(a.enc, r.PayloadEnc)
		a.key = append(a.key, keyID)
		a.ph = append(a.ph, r.PayloadHash[:])
		prev := r.PrevHash
		if prev == nil {
			prev = []byte{}
		}
		a.prev = append(a.prev, prev)
		a.hash = append(a.hash, r.ChainHash)
	}
	touched := map[string]bool{}
	var hc []string
	var hs, ho []int64
	var hh [][]byte
	for i := n - 1; i >= 0; i-- {
		c := recs[i].Chain
		if touched[c] {
			continue
		}
		touched[c] = true
		hc = append(hc, c)
		hs = append(hs, heads[c].seq)
		hh = append(hh, heads[c].hash)
		ho = append(ho, heads[c].offset)
	}
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, insertRecords, a.chain, a.seq, a.eid, a.etype, a.dom, a.bucket,
			a.agg, a.actor, a.corr, a.ch, a.at, a.day, a.topic, a.part, a.off, a.enc, a.key, a.ph, a.prev, a.hash); err != nil {
			return fmt.Errorf("insert records: %w", err)
		}
		if _, err := tx.Exec(ctx, upsertHeads, hc, hs, hh, ho); err != nil {
			return fmt.Errorf("upsert heads: %w", err)
		}
		return nil
	})
}

// existing returns which of ids are already stored.
func existing(ctx context.Context, db *pgxpool.Pool, ids []string) (map[string]bool, error) {
	rows, err := db.Query(ctx, `SELECT event_id FROM audit_records WHERE event_id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// verifyChains rebuilds every chain from sequence 1, decrypting each payload
// and checking its hash. It returns the number of records checked and the
// first broken link found, if any.
func verifyChains(ctx context.Context, db *pgxpool.Pool, g *generator) (int64, string, error) {
	rows, err := db.Query(ctx, `SELECT chain_partition FROM chain_heads ORDER BY 1`)
	if err != nil {
		return 0, "", err
	}
	chains, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, "", err
	}
	var total int64
	var mu sync.Mutex
	var broken []string
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	var firstErr error
	for _, c := range chains {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			n, b, err := verifyChain(ctx, db, g, c)
			mu.Lock()
			defer mu.Unlock()
			total += n
			if b != "" {
				broken = append(broken, b)
			}
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}()
	}
	wg.Wait()
	sort.Strings(broken)
	if len(broken) > 3 {
		broken = append(broken[:3], fmt.Sprintf("and %d more chains", len(broken)-3))
	}
	return total, strings.Join(broken, "; "), firstErr
}

func verifyChain(ctx context.Context, db *pgxpool.Pool, g *generator, chain string) (int64, string, error) {
	rows, err := db.Query(ctx, `SELECT sequence_no, event_id, event_type, occurred_at, kafka_topic, kafka_partition,
		kafka_offset, payload_enc, payload_sha256, prev_hash, chain_hash
		FROM audit_records WHERE chain_partition = $1 ORDER BY sequence_no`, chain)
	if err != nil {
		return 0, "", err
	}
	defer rows.Close()
	var prev []byte
	var n int64
	for rows.Next() {
		r := &record{Chain: chain}
		var ph []byte
		if err := rows.Scan(&r.Seq, &r.EventID, &r.EventType, &r.OccurredAt, &r.Topic, &r.Partition,
			&r.Offset, &r.PayloadEnc, &ph, &r.PrevHash, &r.ChainHash); err != nil {
			return n, "", err
		}
		n++
		if r.Seq != n {
			return n, fmt.Sprintf("%s: gap at seq %d (found %d)", chain, n, r.Seq), nil
		}
		plain, err := g.decrypt(r)
		if err != nil {
			return n, fmt.Sprintf("%s seq %d: payload does not decrypt", chain, r.Seq), nil
		}
		sum := sha256.Sum256(plain)
		if !slices.Equal(sum[:], ph) {
			return n, fmt.Sprintf("%s seq %d: payload hash mismatch", chain, r.Seq), nil
		}
		copy(r.PayloadHash[:], ph)
		if !slices.Equal(r.PrevHash, prev) {
			return n, fmt.Sprintf("%s seq %d: prev_hash does not match", chain, r.Seq), nil
		}
		if want := chainHash(prev, r); !slices.Equal(want, r.ChainHash) {
			return n, fmt.Sprintf("%s seq %d: chain_hash mismatch (row altered)", chain, r.Seq), nil
		}
		prev = r.ChainHash
	}
	return n, "", rows.Err()
}

// searchYSQL runs one metadata query and returns its row count.
type ysqlQuery struct {
	name string
	run  func(ctx context.Context, db *pgxpool.Pool, key string) (int, error)
}

func countRows(rows pgx.Rows) (int, error) {
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}

var ysqlQueries = []ysqlQuery{
	{"aggregate", func(ctx context.Context, db *pgxpool.Pool, k string) (int, error) {
		rows, err := db.Query(ctx, `SELECT chain_partition, sequence_no, event_type, occurred_at FROM audit_records
			WHERE aggregate_id = $1 ORDER BY occurred_at DESC LIMIT 50`, k)
		if err != nil {
			return 0, err
		}
		return countRows(rows)
	}},
	{"correlation", func(ctx context.Context, db *pgxpool.Pool, k string) (int, error) {
		rows, err := db.Query(ctx, `SELECT chain_partition, sequence_no, event_type, occurred_at FROM audit_records
			WHERE correlation_id = $1 ORDER BY occurred_at DESC LIMIT 50`, k)
		if err != nil {
			return 0, err
		}
		return countRows(rows)
	}},
	{"actor_7d", func(ctx context.Context, db *pgxpool.Pool, k string) (int, error) {
		today := time.Now().UTC().Truncate(24 * time.Hour)
		days := make([]time.Time, 7)
		for i := range days {
			days[i] = today.AddDate(0, 0, -i)
		}
		rows, err := db.Query(ctx, `SELECT chain_partition, sequence_no, event_type, occurred_at FROM audit_records
			WHERE actor_id = $1 AND day = ANY($2::date[]) ORDER BY occurred_at DESC LIMIT 50`, k, days)
		if err != nil {
			return 0, err
		}
		return countRows(rows)
	}},
	{"type_prefix_1d", func(ctx context.Context, db *pgxpool.Pool, k string) (int, error) {
		// k is "<domain>|<event type prefix>". The domain index is bucketed, so
		// the query fans out over the buckets and merges the newest 50.
		domain, prefix, _ := strings.Cut(k, "|")
		today := time.Now().UTC().Truncate(24 * time.Hour)
		type hit struct{ at time.Time }
		var mu sync.Mutex
		var hits []hit
		var wg sync.WaitGroup
		errs := make([]error, domainBuckets)
		for b := range domainBuckets {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rows, err := db.Query(ctx, `SELECT occurred_at FROM audit_records
					WHERE domain = $1 AND day = $2 AND domain_bucket = $3 AND event_type LIKE $4
					ORDER BY occurred_at DESC LIMIT 50`, domain, today, int16(b), prefix+"%")
				if err != nil {
					errs[b] = err
					return
				}
				defer rows.Close()
				for rows.Next() {
					var at time.Time
					if err := rows.Scan(&at); err != nil {
						errs[b] = err
						return
					}
					mu.Lock()
					hits = append(hits, hit{at})
					mu.Unlock()
				}
				errs[b] = rows.Err()
			}()
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			return 0, err
		}
		sort.Slice(hits, func(i, j int) bool { return hits[i].at.After(hits[j].at) })
		return min(len(hits), 50), nil
	}},
}
