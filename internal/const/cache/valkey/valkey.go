// Package valkey builds a go-redis client pointed at a Valkey server (Valkey
// is wire-compatible with Redis).
package valkey

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/hunderaweke/dps-audit-service/config"
)

func NewClient(ctx context.Context, cfg config.Valkey) (*redis.Client, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse valkey url: %w", err)
	}
	client := redis.NewClient(opts)
	if err := redisotel.InstrumentTracing(client); err != nil {
		return nil, fmt.Errorf("instrument valkey: %w", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping valkey: %w", err)
	}
	return client, nil
}
