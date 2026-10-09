package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apperrors "github.com/username/example-service/internal/const/errors"
	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/module"
	"github.com/username/example-service/internal/storage/repository/db"
)

type example struct {
	q db.Querier
}

var _ module.ExampleRepository = (*example)(nil)

// NewExample accepts the sqlc Querier, which both *db.Queries (pool) and
// db.New(tx) satisfy, so it can be used inside transactions too.
func NewExample(q db.Querier) module.ExampleRepository {
	return &example{q: q}
}

func (r *example) Create(ctx context.Context, ex models.Example) (models.Example, error) {
	row, err := r.q.CreateExample(ctx, db.CreateExampleParams{
		ID:          ex.ID,
		Name:        ex.Name,
		Description: ex.Description,
		OwnerID:     ex.OwnerID,
		Status:      string(ex.Status),
	})
	if err != nil {
		return models.Example{}, apperrors.ErrDBWrite.Wrap(err, "create example")
	}
	return toModel(row), nil
}

func (r *example) Get(ctx context.Context, id uuid.UUID) (models.Example, error) {
	row, err := r.q.GetExample(ctx, id)
	if err != nil {
		return models.Example{}, mapReadErr(err, id)
	}
	return toModel(row), nil
}

func (r *example) List(ctx context.Context, page models.Page) ([]models.Example, error) {
	rows, err := r.q.ListExamples(ctx, db.ListExamplesParams{Limit: page.Limit, Offset: page.Offset})
	if err != nil {
		return nil, apperrors.ErrDBRead.Wrap(err, "list examples")
	}
	out := make([]models.Example, 0, len(rows))
	for _, row := range rows {
		out = append(out, toModel(row))
	}
	return out, nil
}

func (r *example) UpdateStatus(ctx context.Context, id uuid.UUID, status models.ExampleStatus) (models.Example, error) {
	row, err := r.q.UpdateExampleStatus(ctx, db.UpdateExampleStatusParams{ID: id, Status: string(status)})
	if err != nil {
		return models.Example{}, mapReadErr(err, id)
	}
	return toModel(row), nil
}

func mapReadErr(err error, id uuid.UUID) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return apperrors.ErrNotFound.New("example %s not found", id)
	}
	return apperrors.ErrDBRead.Wrap(err, "read example %s", id)
}

func toModel(row db.Example) models.Example {
	return models.Example{
		ID:          row.ID,
		Name:        row.Name,
		Description: row.Description,
		OwnerID:     row.OwnerID,
		Status:      models.ExampleStatus(row.Status),
		CreatedAt:   row.CreatedAt,
		UpdatedAt:   row.UpdatedAt,
	}
}
