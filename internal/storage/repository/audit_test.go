package repository_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/internal/const/database/mongo"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/storage/repository"
)

// Integration test for the Mongo adapter; requires docker, skipped with -short.
func TestAuditRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("requires docker")
	}
	ctx := context.Background()
	ctr, err := tcmongo.Run(ctx, "mongo:8")
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	uri, err := ctr.ConnectionString(ctx)
	require.NoError(t, err)

	client, database, err := mongo.NewClient(ctx, config.Mongo{URI: uri, Database: "test"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	store := repository.NewAudit(database)
	require.NoError(t, store.Record(ctx, models.AuditEntry{EntityID: "ex_1", Action: "example.processed"}))

	var got models.AuditEntry
	require.NoError(t, database.Collection("audit_log").FindOne(ctx, bson.M{"entity_id": "ex_1"}).Decode(&got))
	assert.Equal(t, "example.processed", got.Action)
	assert.False(t, got.CreatedAt.IsZero())
}
