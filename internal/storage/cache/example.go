package cache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	apperrors "github.com/username/example-service/internal/const/errors"
	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/module"
)

const examplePrefix = "example:"

type example struct {
	client redis.Cmdable
	ttl    time.Duration
}

var _ module.ExampleCache = (*example)(nil)

func NewExample(client redis.Cmdable, ttl time.Duration) module.ExampleCache {
	return &example{client: client, ttl: ttl}
}

func (c *example) Get(ctx context.Context, id uuid.UUID) (models.Example, bool, error) {
	raw, err := c.client.Get(ctx, key(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return models.Example{}, false, nil
	}
	if err != nil {
		return models.Example{}, false, apperrors.ErrCache.Wrap(err, "get example")
	}
	var ex models.Example
	if err := json.Unmarshal(raw, &ex); err != nil {
		return models.Example{}, false, apperrors.ErrCache.Wrap(err, "decode example")
	}
	return ex, true, nil
}

func (c *example) Set(ctx context.Context, ex models.Example) error {
	raw, err := json.Marshal(ex)
	if err != nil {
		return apperrors.ErrCache.Wrap(err, "encode example")
	}
	if err := c.client.Set(ctx, key(ex.ID), raw, c.ttl).Err(); err != nil {
		return apperrors.ErrCache.Wrap(err, "set example")
	}
	return nil
}

func (c *example) Delete(ctx context.Context, id uuid.UUID) error {
	if err := c.client.Del(ctx, key(id)).Err(); err != nil {
		return apperrors.ErrCache.Wrap(err, "delete example")
	}
	return nil
}

func key(id uuid.UUID) string { return examplePrefix + id.String() }
