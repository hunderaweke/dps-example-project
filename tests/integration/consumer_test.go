// Package integration runs the real audit worker (initiator.BuildWorker)
// in-process against Redpanda and Postgres started by testcontainers.
// Requires Docker; skipped with -short.
package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/initiator"
	"github.com/hunderaweke/dps-audit-service/internal/const/events"
	commonv1 "github.com/hunderaweke/dps-audit-service/pkg/dpsapi/gen/dps/common/v1"
	eventsv1 "github.com/hunderaweke/dps-audit-service/pkg/dpsapi/gen/dps/events/v1"
)

const group = "audit.universal.test"

var topics = []string{"ledger.events", "payment.events", "auth.events"}

type harness struct {
	t        *testing.T
	cfg      *config.Config
	db       *pgxpool.Pool
	producer *kgo.Client
	admin    *kadm.Client
}

func TestAuditConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("requires docker")
	}
	h := setup(t)
	ctx := context.Background()

	stop := h.startWorker()

	// 290 unique events across 3 topics x 3 partitions, 10 re-published
	// duplicates, and one value that is not a DPS event at all.
	h.produce(0, 290)
	h.produceDuplicates(10)
	garbage := h.produceRaw("ledger.events", []byte("not a dps event"), "ledger.operation.completed.v1")
	h.waitForRows(291)

	t.Run("an undecodable record is kept, not dropped", func(t *testing.T) {
		var valid bool
		var eventType string
		var payload []byte
		id := fmt.Sprintf("kafka:%s/%d/%d", garbage.Topic, garbage.Partition, garbage.Offset)
		require.NoError(t, h.db.QueryRow(ctx,
			`SELECT metadata_valid, event_type, payload FROM audit_records WHERE event_id = $1`, id).
			Scan(&valid, &eventType, &payload))
		assert.False(t, valid)
		assert.Equal(t, "ledger.operation.completed.v1", eventType, "type comes from the header")
		assert.Equal(t, []byte("not a dps event"), payload)
	})

	t.Run("metadata is stored column by column", func(t *testing.T) {
		var r struct {
			eventType, aggregate, producer, impl, corr, actorType, actorID, topic string
			version                                                               int64
			occurred                                                              time.Time
			valid                                                                 bool
		}
		require.NoError(t, h.db.QueryRow(ctx, `SELECT event_type, aggregate_id, producer, producer_impl,
			correlation_id, actor_type, actor_id, kafka_topic, aggregate_version, occurred_at, metadata_valid
			FROM audit_records WHERE event_id = 'evt-0007'`).
			Scan(&r.eventType, &r.aggregate, &r.producer, &r.impl, &r.corr, &r.actorType, &r.actorID,
				&r.topic, &r.version, &r.occurred, &r.valid))
		assert.True(t, r.valid)
		assert.Equal(t, "payment.payment.completed.v1", r.eventType)
		assert.Equal(t, "agg-07", r.aggregate)
		assert.Equal(t, "payment", r.producer)
		assert.Equal(t, "real", r.impl)
		assert.Equal(t, "corr-0007", r.corr)
		assert.Equal(t, "customer", r.actorType)
		assert.Equal(t, "cust-0007", r.actorID)
		assert.Equal(t, "payment.events", r.topic)
		assert.EqualValues(t, 7, r.version)
		assert.True(t, occurredAt(7).Equal(r.occurred))
	})

	t.Run("offsets are committed once stored", func(t *testing.T) {
		h.waitForLagZero()
	})

	t.Run("a restart resumes from the committed offsets without duplicates", func(t *testing.T) {
		stop()
		h.produce(290, 50)
		h.produceDuplicates(5)
		stop = h.startWorker()
		h.waitForRows(341)
		h.waitForLagZero()
	})

	t.Run("a store outage is retried, nothing is lost", func(t *testing.T) {
		_, err := h.db.Exec(ctx, `ALTER TABLE audit_records RENAME TO audit_records_offline`)
		require.NoError(t, err)
		h.produce(340, 20)
		time.Sleep(3 * time.Second) // the consumer fails and retries meanwhile

		// Nothing is committed while writes fail, so the 20 records stay pending.
		var stored int
		require.NoError(t, h.db.QueryRow(ctx, `SELECT count(*) FROM audit_records_offline`).Scan(&stored))
		assert.Equal(t, 341, stored)
		assert.EqualValues(t, 20, h.lag(), "offsets must not advance while the store is down")

		_, err = h.db.Exec(ctx, `ALTER TABLE audit_records_offline RENAME TO audit_records`)
		require.NoError(t, err)
		h.waitForRows(361)
		h.waitForLagZero()
	})

	stop()
}

func setup(t *testing.T) *harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // first run pulls images
	defer cancel()

	pg, err := tcpostgres.Run(ctx, "postgres:17-alpine", tcpostgres.WithDatabase("audit"), tcpostgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, pg)
	require.NoError(t, err)
	pgURL, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	rp, err := tcredpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v25.2.1")
	testcontainers.CleanupContainer(t, rp)
	require.NoError(t, err)
	broker, err := rp.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	admin := kadm.NewClient(producer)
	_, err = admin.CreateTopics(ctx, 3, 1, nil, topics...)
	require.NoError(t, err)

	cfg := &config.Config{
		App:      config.App{Name: "dps-audit-service", Environment: "test", Version: "integration"},
		Server:   config.Server{ShutdownTimeout: 5 * time.Second},
		Postgres: config.Postgres{URL: pgURL, MaxConns: 10, AutoMigrate: true},
		Kafka: config.Kafka{
			Brokers:       []string{broker},
			ConsumerGroup: group,
			Topics:        config.Topics{Ledger: topics[0], Payment: topics[1], Auth: topics[2]},
		},
		Audit: config.Audit{BatchSize: 25, MaxPollRecords: 200, RetryMaxBackoff: time.Second},
	}

	// The worker applies the migrations; connect for assertions afterwards.
	h := &harness{t: t, cfg: cfg, producer: producer, admin: admin}
	stop := h.startWorker()
	stop()
	h.db, err = pgxpool.New(ctx, pgURL)
	require.NoError(t, err)
	t.Cleanup(h.db.Close)
	return h
}

// startWorker runs the real worker until the returned stop is called.
func (h *harness) startWorker() (stop func()) {
	h.t.Helper()
	ctx := context.Background()
	w, err := initiator.BuildWorker(ctx, h.cfg, zaptest.NewLogger(h.t, zaptest.Level(zap.ErrorLevel)))
	require.NoError(h.t, err)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()
	return func() {
		cancel()
		require.NoError(h.t, <-done)
		require.NoError(h.t, w.Close(ctx))
	}
}

func occurredAt(i int) time.Time {
	return time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second)
}

// event builds DPS event i, Schema Registry framed. Event i goes to
// topics[i%3] so the three domains interleave.
func event(i int) (topic, eventType string, key, value []byte) {
	domains := []string{"ledger", "payment", "auth"}
	types := []string{"ledger.operation.completed.v1", "payment.payment.completed.v1", "auth.session.started.v1"}
	d := i % 3
	meta := &eventsv1.EventMetadata{
		EventId:          fmt.Sprintf("evt-%04d", i),
		EventType:        types[d],
		AggregateId:      fmt.Sprintf("agg-%02d", i%20),
		AggregateVersion: int64(i),
		OccurredAt:       timestamppb.New(occurredAt(i)),
		Producer:         domains[d],
		ProducerImpl:     eventsv1.ProducerImpl_PRODUCER_IMPL_REAL,
		CorrelationId:    fmt.Sprintf("corr-%04d", i),
		Actor:            &commonv1.Actor{Type: commonv1.ActorType_ACTOR_TYPE_CUSTOMER, Id: fmt.Sprintf("cust-%04d", i)},
	}
	m, err := proto.Marshal(meta)
	if err != nil {
		panic(err)
	}
	msg := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), m)
	msg = protowire.AppendBytes(protowire.AppendTag(msg, 2, protowire.BytesType), []byte("body"))
	value = append([]byte{0, 0, 0, 0, 1, 0}, msg...) // magic, schema id 1, message index [0]
	return topics[d], types[d], []byte(meta.AggregateId), value
}

func (h *harness) produce(from, n int) {
	h.t.Helper()
	recs := make([]*kgo.Record, 0, n)
	for i := from; i < from+n; i++ {
		topic, typ, key, value := event(i)
		recs = append(recs, &kgo.Record{Topic: topic, Key: key, Value: value,
			Headers: []kgo.RecordHeader{{Key: events.HeaderEventType, Value: []byte(typ)}}})
	}
	require.NoError(h.t, h.producer.ProduceSync(context.Background(), recs...).FirstErr())
}

// produceDuplicates re-publishes the first n events, as a retried outbox
// relay would: same event_id, new offsets.
func (h *harness) produceDuplicates(n int) { h.produce(0, n) }

func (h *harness) produceRaw(topic string, value []byte, eventType string) *kgo.Record {
	h.t.Helper()
	rec := &kgo.Record{Topic: topic, Value: value, Headers: []kgo.RecordHeader{{Key: events.HeaderEventType, Value: []byte(eventType)}}}
	require.NoError(h.t, h.producer.ProduceSync(context.Background(), rec).FirstErr())
	return rec
}

func (h *harness) waitForRows(want int) {
	h.t.Helper()
	var got int
	require.Eventually(h.t, func() bool {
		err := h.db.QueryRow(context.Background(), `SELECT count(*) FROM audit_records`).Scan(&got)
		return err == nil && got == want
	}, 90*time.Second, 250*time.Millisecond, "audit_records rows: want %d, last saw %d", want, got)
	// And no more arrive: nothing was double-counted.
	time.Sleep(time.Second)
	require.NoError(h.t, h.db.QueryRow(context.Background(), `SELECT count(*) FROM audit_records`).Scan(&got))
	require.Equal(h.t, want, got)
}

// lag is the group's uncommitted record count, or -1 if it cannot be read.
func (h *harness) lag() int64 {
	lags, err := h.admin.Lag(context.Background(), group)
	if err != nil {
		return -1
	}
	l, ok := lags[group]
	if !ok || l.Error() != nil {
		return -1
	}
	return l.Lag.Total()
}

func (h *harness) waitForLagZero() {
	h.t.Helper()
	var total int64 = -1
	require.Eventually(h.t, func() bool {
		total = h.lag()
		return total == 0
	}, 30*time.Second, 250*time.Millisecond, "consumer group lag: last saw %d", total)
}
