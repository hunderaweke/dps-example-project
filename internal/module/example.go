// Package module is the application core. It declares the ports (interfaces)
// it needs and implements the use cases on top of them. It must not import
// transport or storage packages (gin, huma, pgx, go-redis, franz-go, temporal,
// grpc); those are adapters that implement or call these ports and are wired
// together in initiator/.
package module

import (
	"context"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"go.uber.org/zap"

	apperrors "github.com/hunderaweke/dps-audit-service/internal/const/errors"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
)

// ---- Inbound port (driven by router, Kafka handler, Temporal activities) ----

type Example interface {
	Create(ctx context.Context, ex models.Example) (models.Example, error)
	Get(ctx context.Context, id uuid.UUID) (models.Example, error)
	List(ctx context.Context, page models.Page) ([]models.Example, error)

	// HandleCreated reacts to an ExampleCreatedEvent by starting processing.
	HandleCreated(ctx context.Context, ev models.ExampleCreatedEvent) error
	// VerifyOwner checks with the account service that the owner is active.
	VerifyOwner(ctx context.Context, id uuid.UUID) error
	// MarkProcessed finalizes processing: status update, audit, cache invalidation.
	MarkProcessed(ctx context.Context, id uuid.UUID) (models.Example, error)
}

// ---- Outbound ports (implemented by adapters in internal/storage, pkg/) ----

type ExampleRepository interface {
	Create(ctx context.Context, ex models.Example) (models.Example, error)
	Get(ctx context.Context, id uuid.UUID) (models.Example, error)
	List(ctx context.Context, page models.Page) ([]models.Example, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status models.ExampleStatus) (models.Example, error)
}

type ExampleCache interface {
	// Get returns found=false on a cache miss.
	Get(ctx context.Context, id uuid.UUID) (ex models.Example, found bool, err error)
	Set(ctx context.Context, ex models.Example) error
	Delete(ctx context.Context, id uuid.UUID) error
}

type EventPublisher interface {
	PublishExampleCreated(ctx context.Context, ev models.ExampleCreatedEvent) error
}

type WorkflowStarter interface {
	StartProcessExample(ctx context.Context, ev models.ExampleCreatedEvent) error
}

type AccountClient interface {
	GetAccount(ctx context.Context, id string) (models.Account, error)
}

type AuditStore interface {
	Record(ctx context.Context, entry models.AuditEntry) error
}

// ---- Implementation ----

type ExampleDeps struct {
	Repo      ExampleRepository
	Cache     ExampleCache
	Publisher EventPublisher
	Workflows WorkflowStarter
	Accounts  AccountClient
	Audit     AuditStore
	Logger    *zap.Logger
}

type example struct {
	ExampleDeps
	validate *validator.Validate
}

func NewExample(deps ExampleDeps) Example {
	if deps.Logger == nil {
		deps.Logger = zap.NewNop()
	}
	return &example{
		ExampleDeps: deps,
		validate:    validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (s *example) Create(ctx context.Context, ex models.Example) (models.Example, error) {
	ex.ID = uuid.New()
	ex.Status = models.ExampleStatusPending
	if err := s.validateStruct(ex); err != nil {
		return models.Example{}, err
	}

	created, err := s.Repo.Create(ctx, ex)
	if err != nil {
		return models.Example{}, err
	}

	// Cache and publish failures do not fail the request. For guaranteed
	// delivery replace the direct publish with a transactional outbox.
	if err := s.Cache.Set(ctx, created); err != nil {
		s.Logger.Warn("cache example", zap.Stringer("id", created.ID), zap.Error(err))
	}
	ev := models.ExampleCreatedEvent{ID: created.ID, OwnerID: created.OwnerID}
	if err := s.Publisher.PublishExampleCreated(ctx, ev); err != nil {
		s.Logger.Error("publish example created", zap.Stringer("id", created.ID), zap.Error(err))
	}
	return created, nil
}

func (s *example) Get(ctx context.Context, id uuid.UUID) (models.Example, error) {
	if ex, found, err := s.Cache.Get(ctx, id); err != nil {
		s.Logger.Warn("read example cache", zap.Stringer("id", id), zap.Error(err))
	} else if found {
		return ex, nil
	}

	ex, err := s.Repo.Get(ctx, id)
	if err != nil {
		return models.Example{}, err
	}
	if err := s.Cache.Set(ctx, ex); err != nil {
		s.Logger.Warn("cache example", zap.Stringer("id", id), zap.Error(err))
	}
	return ex, nil
}

func (s *example) List(ctx context.Context, page models.Page) ([]models.Example, error) {
	if page.Limit <= 0 || page.Limit > 100 {
		page.Limit = 20
	}
	if page.Offset < 0 {
		page.Offset = 0
	}
	return s.Repo.List(ctx, page)
}

func (s *example) HandleCreated(ctx context.Context, ev models.ExampleCreatedEvent) error {
	if err := s.validateStruct(ev); err != nil {
		return err
	}
	return s.Workflows.StartProcessExample(ctx, ev)
}

func (s *example) VerifyOwner(ctx context.Context, id uuid.UUID) error {
	ex, err := s.Repo.Get(ctx, id)
	if err != nil {
		return err
	}
	acc, err := s.Accounts.GetAccount(ctx, ex.OwnerID)
	if err != nil {
		return err
	}
	if !acc.Active {
		return apperrors.ErrForbidden.New("owner account %s is not active", acc.ID)
	}
	return nil
}

func (s *example) MarkProcessed(ctx context.Context, id uuid.UUID) (models.Example, error) {
	ex, err := s.Repo.UpdateStatus(ctx, id, models.ExampleStatusProcessed)
	if err != nil {
		return models.Example{}, err
	}
	if err := s.Audit.Record(ctx, models.AuditEntry{
		EntityID: id.String(),
		Action:   "example.processed",
		Metadata: map[string]string{"owner_id": ex.OwnerID},
	}); err != nil {
		return models.Example{}, err
	}
	if err := s.Cache.Delete(ctx, id); err != nil {
		s.Logger.Warn("invalidate example cache", zap.Stringer("id", id), zap.Error(err))
	}
	return ex, nil
}

func (s *example) validateStruct(v any) error {
	if err := s.validate.Struct(v); err != nil {
		return apperrors.ErrInvalidInput.New("validation failed: %v", err)
	}
	return nil
}
