// Package models holds the domain entities. It is the innermost layer and must
// not import any other internal package or infrastructure library.
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

// Example is the domain entity. The validate tags are checked by the module
// layer, independently of any transport-level validation.
type Example struct {
	ID          uuid.UUID     `json:"id"`
	Name        string        `json:"name" validate:"required,min=3,max=100"`
	Description string        `json:"description" validate:"max=500"`
	OwnerID     string        `json:"owner_id" validate:"required"`
	Status      ExampleStatus `json:"status" validate:"oneof=pending processed"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

// ExampleCreatedEvent is published when an example is created and consumed by
// the worker to start processing.
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

// AuditEntry records something that happened to an entity (stored in Mongo).
type AuditEntry struct {
	EntityID  string            `bson:"entity_id"`
	Action    string            `bson:"action"`
	Metadata  map[string]string `bson:"metadata,omitempty"`
	CreatedAt time.Time         `bson:"created_at"`
}

// Page is a generic pagination request used across modules.
type Page struct {
	Limit  int32
	Offset int32
}
