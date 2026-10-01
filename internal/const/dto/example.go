// Package dto holds transport-level request/response shapes. Huma reads the
// struct tags (path, query, minLength, ...) to validate input at the edge and
// to generate the OpenAPI document. DTOs are converted to/from domain models
// in the router so the module layer never sees them.
package dto

import (
	"time"

	"github.com/google/uuid"

	"github.com/username/example-service/internal/const/models"
)

type CreateExampleBody struct {
	Name        string `json:"name" minLength:"3" maxLength:"100" example:"my example" doc:"Human readable name"`
	Description string `json:"description,omitempty" maxLength:"500" doc:"Optional description"`
	OwnerID     string `json:"owner_id" minLength:"1" example:"acc_123" doc:"Account that owns the example"`
}

type CreateExampleRequest struct {
	Body CreateExampleBody
}

type GetExampleRequest struct {
	ID uuid.UUID `path:"id" doc:"Example ID"`
}

type ListExamplesRequest struct {
	Limit  int32 `query:"limit" minimum:"1" maximum:"100" default:"20"`
	Offset int32 `query:"offset" minimum:"0" default:"0"`
}

type Example struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	OwnerID     string    `json:"owner_id"`
	Status      string    `json:"status" enum:"pending,processed"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ExampleResponse struct {
	Body Example
}

type ListExamplesResponse struct {
	Body struct {
		Items  []Example `json:"items"`
		Limit  int32     `json:"limit"`
		Offset int32     `json:"offset"`
	}
}

type HealthResponse struct {
	Body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks,omitempty"`
	}
}

func (b CreateExampleBody) ToModel() models.Example {
	return models.Example{Name: b.Name, Description: b.Description, OwnerID: b.OwnerID}
}

func ExampleFromModel(m models.Example) Example {
	return Example{
		ID:          m.ID,
		Name:        m.Name,
		Description: m.Description,
		OwnerID:     m.OwnerID,
		Status:      string(m.Status),
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
	}
}
