package models

import (
	"time"

	"github.com/google/uuid"
)

type ExampleStatus string

const (
	ExampleStatusPending   ExampleStatus = "pending"
	ExampleStatusProcessed ExampleStatus = "processed"
)

type Example struct {
	ID          uuid.UUID     `json:"id"`
	Name        string        `json:"name" validate:"required,min=3,max=100"`
	Description string        `json:"description" validate:"max=500"`
	OwnerID     string        `json:"owner_id" validate:"required"`
	Status      ExampleStatus `json:"status" validate:"oneof=pending processed"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

type ExampleCreatedEvent struct {
	ID      uuid.UUID `json:"id" validate:"required"`
	OwnerID string    `json:"owner_id" validate:"required"`
}

// Account is the subset of the external account service that this domain cares about.
type Account struct {
	ID     string
	Name   string
	Active bool
}

type AuditEntry struct {
	EntityID  string            `bson:"entity_id"`
	Action    string            `bson:"action"`
	Metadata  map[string]string `bson:"metadata,omitempty"`
	CreatedAt time.Time         `bson:"created_at"`
}

type Page struct {
	Limit  int32
	Offset int32
}
