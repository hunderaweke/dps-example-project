// Package errors defines the application error taxonomy using errorx.
//
// Adapters wrap infrastructure errors into one of these types (e.g. a pgx
// ErrNoRows becomes ErrNotFound). The module layer returns them unchanged, and
// inbound adapters translate them via HTTPStatus. No layer outside the adapters
// needs to know about driver-specific errors.
package errors

import (
	"net/http"

	"github.com/joomcode/errorx"
)

var (
	ns = errorx.NewNamespace("app")

	ErrNotFound     = ns.NewType("not_found", errorx.NotFound())
	ErrInvalidInput = ns.NewType("invalid_input")
	ErrConflict     = ns.NewType("conflict", errorx.Duplicate())
	ErrUnauthorized = ns.NewType("unauthorized")
	ErrForbidden    = ns.NewType("forbidden")
	ErrUnavailable  = ns.NewType("unavailable", errorx.Temporary())
	ErrInternal     = ns.NewType("internal")

	// Storage-specific subtypes still map to their parent type.
	ErrDBRead  = ErrInternal.NewSubtype("db_read")
	ErrDBWrite = ErrInternal.NewSubtype("db_write")
	ErrCache   = ErrInternal.NewSubtype("cache")
	ErrPublish = ErrInternal.NewSubtype("publish")
)

// HTTPStatus maps an application error to an HTTP status code.
func HTTPStatus(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errorx.IsOfType(err, ErrNotFound):
		return http.StatusNotFound
	case errorx.IsOfType(err, ErrInvalidInput):
		return http.StatusUnprocessableEntity
	case errorx.IsOfType(err, ErrConflict):
		return http.StatusConflict
	case errorx.IsOfType(err, ErrUnauthorized):
		return http.StatusUnauthorized
	case errorx.IsOfType(err, ErrForbidden):
		return http.StatusForbidden
	case errorx.IsOfType(err, ErrUnavailable):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// PublicMessage returns a message that is safe to show to clients. Internal
// errors are hidden behind a generic message.
func PublicMessage(err error) string {
	if HTTPStatus(err) >= http.StatusInternalServerError {
		return "internal server error"
	}
	if e := errorx.Cast(err); e != nil {
		return e.Message()
	}
	return err.Error()
}
