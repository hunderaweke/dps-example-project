package initiator

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/internal/const/cache/valkey"
	"github.com/hunderaweke/dps-audit-service/internal/const/database/postgres"
	"github.com/hunderaweke/dps-audit-service/internal/const/messaging/kafka"
	"github.com/hunderaweke/dps-audit-service/internal/const/workflow/temporal"
)

// needs selects which platform clients a process connects to, so the API does
// not depend on infrastructure only the worker uses.
type needs struct {
	Postgres, Valkey, Producer, Temporal bool
}

var (
	apiNeeds    = needs{Postgres: true}
	workerNeeds = needs{Postgres: true}
)

// Platform holds infrastructure clients. Fields not selected by needs are nil.
type Platform struct {
	Postgres *pgxpool.Pool
	Valkey   *redis.Client
	Producer *kgo.Client
	Temporal client.Client

	closers []func(context.Context) error
}

func newPlatform(ctx context.Context, cfg *config.Config, logger *zap.Logger, n needs) (_ *Platform, err error) {
	p := &Platform{}
	defer func() {
		if err != nil {
			_ = p.Close(context.Background())
		}
	}()

	if n.Postgres {
		if cfg.Postgres.AutoMigrate {
			if err := postgres.Migrate(cfg.Postgres.URL); err != nil {
				return nil, err
			}
			logger.Info("database migrations applied")
		}
		if p.Postgres, err = postgres.NewPool(ctx, cfg.Postgres); err != nil {
			return nil, err
		}
		p.onClose(func(context.Context) error { p.Postgres.Close(); return nil })
	}
	if n.Valkey {
		if p.Valkey, err = valkey.NewClient(ctx, cfg.Valkey); err != nil {
			return nil, err
		}
		p.onClose(func(context.Context) error { return p.Valkey.Close() })
	}
	if n.Producer {
		if p.Producer, err = kafka.NewProducer(ctx, cfg.Kafka); err != nil {
			return nil, err
		}
		p.onClose(func(ctx context.Context) error { _ = p.Producer.Flush(ctx); p.Producer.Close(); return nil })
	}
	if n.Temporal {
		if p.Temporal, err = temporal.NewClient(cfg.Temporal, logger); err != nil {
			return nil, err
		}
		p.onClose(func(context.Context) error { p.Temporal.Close(); return nil })
	}
	return p, nil
}

func (p *Platform) onClose(f func(context.Context) error) { p.closers = append(p.closers, f) }

// Close releases clients in reverse order of creation.
func (p *Platform) Close(ctx context.Context) error {
	var errs []error
	for i := len(p.closers) - 1; i >= 0; i-- {
		errs = append(errs, p.closers[i](ctx))
	}
	p.closers = nil
	return errors.Join(errs...)
}

// initTelemetry configures the global tracer provider and W3C propagation.
// Propagators are always set so trace context flows through HTTP, Kafka
// headers and Temporal even when exporting is disabled.
func initTelemetry(ctx context.Context, cfg *config.Config) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if !cfg.Telemetry.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Telemetry.OTLPEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", cfg.App.Name),
		attribute.String("service.version", cfg.App.Version),
		attribute.String("deployment.environment", cfg.App.Environment),
	))
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Telemetry.SampleRatio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
