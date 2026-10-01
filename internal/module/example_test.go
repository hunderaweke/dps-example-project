package module_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	apperrors "github.com/username/example-service/internal/const/errors"
	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/module"
	"github.com/username/example-service/internal/module/mocks"
)

type deps struct {
	repo      *mocks.ExampleRepository
	cache     *mocks.ExampleCache
	publisher *mocks.EventPublisher
	workflows *mocks.WorkflowStarter
	accounts  *mocks.AccountClient
	audit     *mocks.AuditStore
}

func newSUT(t *testing.T) (module.Example, deps) {
	t.Helper()
	d := deps{
		repo:      mocks.NewExampleRepository(t),
		cache:     mocks.NewExampleCache(t),
		publisher: mocks.NewEventPublisher(t),
		workflows: mocks.NewWorkflowStarter(t),
		accounts:  mocks.NewAccountClient(t),
		audit:     mocks.NewAuditStore(t),
	}
	return module.NewExample(module.ExampleDeps{
		Repo: d.repo, Cache: d.cache, Publisher: d.publisher,
		Workflows: d.workflows, Accounts: d.accounts, Audit: d.audit,
	}), d
}

func TestCreate(t *testing.T) {
	ctx := context.Background()

	t.Run("persists, caches and publishes", func(t *testing.T) {
		sut, d := newSUT(t)
		input := models.Example{Name: "first", OwnerID: "acc_1"}

		d.repo.EXPECT().Create(ctx, mock.MatchedBy(func(ex models.Example) bool {
			return ex.ID != uuid.Nil && ex.Status == models.ExampleStatusPending && ex.Name == "first"
		})).RunAndReturn(func(_ context.Context, ex models.Example) (models.Example, error) { return ex, nil })
		d.cache.EXPECT().Set(ctx, mock.Anything).Return(nil)
		d.publisher.EXPECT().PublishExampleCreated(ctx, mock.MatchedBy(func(ev models.ExampleCreatedEvent) bool {
			return ev.OwnerID == "acc_1"
		})).Return(nil)

		got, err := sut.Create(ctx, input)
		require.NoError(t, err)
		assert.Equal(t, models.ExampleStatusPending, got.Status)
	})

	t.Run("rejects invalid input before touching storage", func(t *testing.T) {
		sut, _ := newSUT(t)
		_, err := sut.Create(ctx, models.Example{Name: "x"})
		require.Error(t, err)
		assert.True(t, errorx.IsOfType(err, apperrors.ErrInvalidInput))
	})

	t.Run("publish failure does not fail the request", func(t *testing.T) {
		sut, d := newSUT(t)
		d.repo.EXPECT().Create(ctx, mock.Anything).RunAndReturn(func(_ context.Context, ex models.Example) (models.Example, error) { return ex, nil })
		d.cache.EXPECT().Set(ctx, mock.Anything).Return(nil)
		d.publisher.EXPECT().PublishExampleCreated(ctx, mock.Anything).Return(errors.New("broker down"))

		_, err := sut.Create(ctx, models.Example{Name: "first", OwnerID: "acc_1"})
		require.NoError(t, err)
	})
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	id := uuid.New()
	ex := models.Example{ID: id, Name: "cached", OwnerID: "acc_1", Status: models.ExampleStatusPending}

	t.Run("cache hit skips the repository", func(t *testing.T) {
		sut, d := newSUT(t)
		d.cache.EXPECT().Get(ctx, id).Return(ex, true, nil)

		got, err := sut.Get(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, ex, got)
	})

	t.Run("cache miss reads through and populates the cache", func(t *testing.T) {
		sut, d := newSUT(t)
		d.cache.EXPECT().Get(ctx, id).Return(models.Example{}, false, nil)
		d.repo.EXPECT().Get(ctx, id).Return(ex, nil)
		d.cache.EXPECT().Set(ctx, ex).Return(nil)

		got, err := sut.Get(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, ex, got)
	})

	t.Run("not found is propagated", func(t *testing.T) {
		sut, d := newSUT(t)
		d.cache.EXPECT().Get(ctx, id).Return(models.Example{}, false, nil)
		d.repo.EXPECT().Get(ctx, id).Return(models.Example{}, apperrors.ErrNotFound.New("nope"))

		_, err := sut.Get(ctx, id)
		assert.True(t, errorx.IsOfType(err, apperrors.ErrNotFound))
	})
}

func TestList_ClampsPagination(t *testing.T) {
	ctx := context.Background()
	sut, d := newSUT(t)
	d.repo.EXPECT().List(ctx, models.Page{Limit: 20, Offset: 0}).Return(nil, nil)

	_, err := sut.List(ctx, models.Page{Limit: 1000, Offset: -5})
	require.NoError(t, err)
}

func TestVerifyOwner(t *testing.T) {
	ctx := context.Background()
	id := uuid.New()
	ex := models.Example{ID: id, OwnerID: "acc_1"}

	t.Run("active owner passes", func(t *testing.T) {
		sut, d := newSUT(t)
		d.repo.EXPECT().Get(ctx, id).Return(ex, nil)
		d.accounts.EXPECT().GetAccount(ctx, "acc_1").Return(models.Account{ID: "acc_1", Active: true}, nil)
		require.NoError(t, sut.VerifyOwner(ctx, id))
	})

	t.Run("inactive owner is forbidden", func(t *testing.T) {
		sut, d := newSUT(t)
		d.repo.EXPECT().Get(ctx, id).Return(ex, nil)
		d.accounts.EXPECT().GetAccount(ctx, "acc_1").Return(models.Account{ID: "acc_1"}, nil)
		assert.True(t, errorx.IsOfType(sut.VerifyOwner(ctx, id), apperrors.ErrForbidden))
	})
}

func TestMarkProcessed(t *testing.T) {
	ctx := context.Background()
	id := uuid.New()
	sut, d := newSUT(t)

	d.repo.EXPECT().UpdateStatus(ctx, id, models.ExampleStatusProcessed).
		Return(models.Example{ID: id, OwnerID: "acc_1", Status: models.ExampleStatusProcessed}, nil)
	d.audit.EXPECT().Record(ctx, mock.MatchedBy(func(e models.AuditEntry) bool {
		return e.EntityID == id.String() && e.Action == "example.processed"
	})).Return(nil)
	d.cache.EXPECT().Delete(ctx, id).Return(nil)

	got, err := sut.MarkProcessed(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, models.ExampleStatusProcessed, got.Status)
}

func TestHandleCreated(t *testing.T) {
	ctx := context.Background()
	sut, d := newSUT(t)
	ev := models.ExampleCreatedEvent{ID: uuid.New(), OwnerID: "acc_1"}
	d.workflows.EXPECT().StartProcessExample(ctx, ev).Return(nil)

	require.NoError(t, sut.HandleCreated(ctx, ev))

	err := sut.HandleCreated(ctx, models.ExampleCreatedEvent{})
	assert.True(t, errorx.IsOfType(err, apperrors.ErrInvalidInput))
}
