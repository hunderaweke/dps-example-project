package event

import (
	"context"
	"time"

	"github.com/joomcode/errorx"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	apperrors "github.com/hunderaweke/dps-audit-service/internal/const/errors"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
)

// AuditConsumerConfig tunes the audit consumer. Zero values take the defaults.
type AuditConsumerConfig struct {
	BatchSize       int           // records per Audit.Record call (default 500)
	MaxPollRecords  int           // records per poll across partitions (default 5000)
	RetryMaxBackoff time.Duration // cap of the retry backoff (default 30s)
	MaxWorkers      int           // partitions written concurrently per poll (default 10)
}

// AuditConsumer feeds every DPS domain topic into the audit trail. Unlike the
// generic Consumer it never drops a record: a failed write is retried until it
// succeeds or the worker shuts down, and offsets are committed only after every
// record of the poll is stored. Redelivered records are deduplicated by the
// store on event_id. The client must be created with kgo.BlockRebalanceOnPoll
// so partitions are not revoked mid-batch.
type AuditConsumer struct {
	client *kgo.Client
	tracer *kotel.Tracer
	audit  module.Audit
	cfg    AuditConsumerConfig
	logger *zap.Logger
}

func NewAuditConsumer(client *kgo.Client, tracer *kotel.Tracer, audit module.Audit, cfg AuditConsumerConfig, logger *zap.Logger) *AuditConsumer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 5000
	}
	if cfg.RetryMaxBackoff <= 0 {
		cfg.RetryMaxBackoff = 30 * time.Second
	}
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = 10
	}
	return &AuditConsumer{client: client, tracer: tracer, audit: audit, cfg: cfg, logger: logger}
}

// Run polls until ctx is cancelled or the client is closed. Each poll's
// partitions are written concurrently, in order within a partition, then the
// offsets are committed. A shutdown mid-poll commits nothing, so those records
// are read again by the next owner of the partition.
func (c *AuditConsumer) Run(ctx context.Context) error {
	// With BlockRebalanceOnPoll every poll blocks rebalances, and leaving the
	// group, until AllowRebalance; without this, Close hangs after shutdown.
	defer c.client.AllowRebalance()
	for {
		fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Error("fetch error", zap.String("topic", topic), zap.Int32("partition", partition), zap.Error(err))
		})

		if err := c.storeFetches(ctx, fetches); err != nil {
			return nil
		}
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.logger.Error("commit offsets", zap.Error(err))
		}
		c.client.AllowRebalance()
	}
}

// storeFetches writes the poll's partitions concurrently, at most MaxWorkers
// at a time, each in order. It only returns an error when ctx is cancelled.
func (c *AuditConsumer) storeFetches(ctx context.Context, fetches kgo.Fetches) error {
	var g errgroup.Group
	g.SetLimit(c.cfg.MaxWorkers)
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}
		g.Go(func() error { return c.storePartition(ctx, p.Records) })
	})
	return g.Wait()
}

// storePartition writes one partition's records in order, in batches. It only
// returns an error when ctx is cancelled.
func (c *AuditConsumer) storePartition(ctx context.Context, recs []*kgo.Record) error {
	for start := 0; start < len(recs); start += c.cfg.BatchSize {
		chunk := recs[start:min(start+c.cfg.BatchSize, len(recs))]
		batch := make([]models.AuditRecord, len(chunk))
		for i, r := range chunk {
			batch[i] = decodeRecord(r)
		}
		if err := c.storeBatch(ctx, chunk[0], batch); err != nil {
			return err
		}
	}
	return nil
}

func (c *AuditConsumer) storeBatch(ctx context.Context, first *kgo.Record, batch []models.AuditRecord) error {
	// Continue the producer's trace of the batch's first record.
	_, span := c.tracer.WithProcessSpan(first)
	defer span.End()
	ctx = trace.ContextWithSpan(ctx, span)

	log := c.logger.With(zap.String("topic", first.Topic), zap.Int32("partition", first.Partition),
		zap.Int64("first_offset", first.Offset), zap.Int("records", len(batch)))
	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		_, err := c.audit.Record(ctx, batch)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errorx.IsOfType(err, apperrors.ErrInvalidInput) {
			// The decoder always produces valid records; this is a bug. Keep
			// retrying rather than dropping, so the partition stalls visibly.
			log.Error("audit batch rejected as invalid; not dropping", zap.Int("attempt", attempt), zap.Error(err))
		} else {
			log.Warn("store audit batch, retrying", zap.Int("attempt", attempt), zap.Duration("backoff", backoff), zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.cfg.RetryMaxBackoff)
	}
}
