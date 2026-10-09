// Package mongo builds the MongoDB client.
package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/username/example-service/config"
)

func NewClient(ctx context.Context, cfg config.Mongo) (*mongo.Client, *mongo.Database, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, nil, fmt.Errorf("connect mongo: %w", err)
	}
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, nil, fmt.Errorf("ping mongo: %w", err)
	}
	return client, client.Database(cfg.Database), nil
}
