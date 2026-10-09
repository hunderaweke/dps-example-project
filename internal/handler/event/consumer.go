// Package event is the Kafka inbound adapter. consumer.go is the generic poll
// loop; each <domain>.go file decodes records for one topic and calls the
// module (core).
package event

import (
	"context"
	"time"

	"github.com/joomcode/errorx"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apperrors "github.com/username/example-service/internal/const/errors"
)

// HandlerFunc processes one record. Returning an ErrInvalidInput error marks
// the record as poison and it is skipped; other errors are retried.
type HandlerFunc func(ctx context.Context, rec *kgo.Record) error

type Consumer struct {
	client     *kgo.Client
	tracer     *kotel.Tracer
	handlers   map[string]HandlerFunc
	logger     *zap.Logger
	maxRetries int
}

func NewConsumer(client *kgo.Client, tracer *kotel.Tracer, handlers map[string]HandlerFunc, logger *zap.Logger) *Consumer {
	return &Consumer{client: client, tracer: tracer, handlers: handlers, logger: logger, maxRetries: 3}
}

// Topics returns the topics this consumer has handlers for.
func (c *Consumer) Topics() []string {
	topics := make([]string, 0, len(c.handlers))
	for t := range c.handlers {
		topics = append(topics, t)
	}
	return topics
}

// Run polls until ctx is cancelled or the client is closed. Offsets are
// committed after each batch is handled (at-least-once delivery), so handlers
// must be idempotent.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Error("fetch error", zap.String("topic", topic), zap.Int32("partition", partition), zap.Error(err))
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			c.handle(ctx, rec)
		})
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.logger.Error("commit offsets", zap.Error(err))
		}
	}
}

func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) {
	h, ok := c.handlers[rec.Topic]
	if !ok {
		return
	}
	// Continue the producer's trace (propagated via record headers) while
	// keeping ctx's cancellation so shutdown interrupts handlers.
	_, span := c.tracer.WithProcessSpan(rec)
	defer span.End()
	ctx = trace.ContextWithSpan(ctx, span)

	log := c.logger.With(zap.String("topic", rec.Topic), zap.Int32("partition", rec.Partition), zap.Int64("offset", rec.Offset))
	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := h(ctx, rec)
		if err == nil {
			return
		}
		if errorx.IsOfType(err, apperrors.ErrInvalidInput) || attempt > c.maxRetries {
			// Extension point: publish to a dead-letter topic here.
			log.Error("dropping record", zap.Int("attempt", attempt), zap.Error(err))
			return
		}
		log.Warn("handle record, retrying", zap.Int("attempt", attempt), zap.Error(err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
		}
	}
}
