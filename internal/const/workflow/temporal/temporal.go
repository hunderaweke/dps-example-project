package temporal

import (
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.uber.org/zap"

	"github.com/username/example-service/config"
)

func NewClient(cfg config.Temporal, logger *zap.Logger) (client.Client, error) {
	tracing, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{})
	if err != nil {
		return nil, fmt.Errorf("temporal tracing interceptor: %w", err)
	}
	c, err := client.Dial(client.Options{
		HostPort:     cfg.HostPort,
		Namespace:    cfg.Namespace,
		Logger:       zapLogger{logger.Sugar().With("component", "temporal")},
		Interceptors: []interceptor.ClientInterceptor{tracing},
	})
	if err != nil {
		return nil, fmt.Errorf("dial temporal: %w", err)
	}
	return c, nil
}

type zapLogger struct{ s *zap.SugaredLogger }

func (l zapLogger) Debug(msg string, kv ...any) { l.s.Debugw(msg, kv...) }
func (l zapLogger) Info(msg string, kv ...any)  { l.s.Infow(msg, kv...) }
func (l zapLogger) Warn(msg string, kv ...any)  { l.s.Warnw(msg, kv...) }
func (l zapLogger) Error(msg string, kv ...any) { l.s.Errorw(msg, kv...) }
