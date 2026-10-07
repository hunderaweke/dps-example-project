// Package dto holds transport-level request/response shapes. Huma reads the
// struct tags (path, query, minLength, ...) to validate input at the edge and
// to generate the OpenAPI document. DTOs are converted to/from domain models
// in the router so the module layer never sees them.
package dto

type HealthResponse struct {
	Body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks,omitempty"`
	}
}
