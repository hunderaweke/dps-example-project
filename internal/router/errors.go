package router

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apperrors "github.com/username/example-service/internal/const/errors"
)

type errorMapper struct {
	logger *zap.Logger
}

// toHuma converts an application error to an RFC 9457 problem response. 5xx
// details are logged but never returned to the client.
func (m errorMapper) toHuma(ctx context.Context, err error) error {
	status := apperrors.HTTPStatus(err)
	if status >= http.StatusInternalServerError {
		m.logger.Error("request failed",
			zap.String("trace_id", trace.SpanContextFromContext(ctx).TraceID().String()),
			zap.Error(err))
	}
	return huma.NewError(status, apperrors.PublicMessage(err))
}
