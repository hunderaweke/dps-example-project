// Package kafka builds franz-go clients for Kafka/Redpanda with OpenTelemetry
// hooks so trace context propagates through record headers.
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"

	"github.com/username/example-service/config"
)

// Tracer is shared by producer and consumer so consumers can continue the
// producer's trace via Tracer.WithProcessSpan.
var Tracer = kotel.NewTracer()

func hooks() kgo.Opt {
	return kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(Tracer)).Hooks()...)
}

// NewProducer returns a client for producing records.
func NewProducer(ctx context.Context, cfg config.Kafka) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.AllowAutoTopicCreation(),
		hooks(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}
	return client, nil
}

// NewConsumer returns a consumer-group client subscribed to topics. Offsets
// are committed manually after a record is handled successfully.
func NewConsumer(ctx context.Context, cfg config.Kafka, topics ...string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
		hooks(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}
	return client, nil
}
