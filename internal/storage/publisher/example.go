// Package publisher holds event publishing adapters backed by Kafka/Redpanda.
package publisher

import (
	"context"
	"encoding/json"

	"github.com/twmb/franz-go/pkg/kgo"

	apperrors "github.com/username/example-service/internal/const/errors"
	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/module"
)

type example struct {
	client *kgo.Client
	topic  string
}

var _ module.EventPublisher = (*example)(nil)

func NewExample(client *kgo.Client, topic string) module.EventPublisher {
	return &example{client: client, topic: topic}
}

// PublishExampleCreated produces synchronously. The record key is the example
// ID so all events for one example land on the same partition (ordering).
func (p *example) PublishExampleCreated(ctx context.Context, ev models.ExampleCreatedEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return apperrors.ErrPublish.Wrap(err, "encode event")
	}
	rec := &kgo.Record{Topic: p.topic, Key: []byte(ev.ID.String()), Value: payload}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return apperrors.ErrPublish.Wrap(err, "produce %s", p.topic)
	}
	return nil
}
