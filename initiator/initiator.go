package initiator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/internal/const/messaging/kafka"
	"github.com/hunderaweke/dps-audit-service/internal/handler/event"
)

// API bundles everything needed to serve HTTP. Exported so e2e tests can run
// the real API in-process against testcontainers.
type API struct {
	Handler  http.Handler
	Platform *Platform
	Modules  Modules
}

func BuildAPI(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*API, error) {
	p, err := newPlatform(ctx, cfg, logger, apiNeeds)
	if err != nil {
		return nil, err
	}
	mods := newModules(cfg, p, logger)
	return &API{Handler: newHTTPHandler(cfg, p, mods, logger), Platform: p, Modules: mods}, nil
}

// InitiateAPI runs the HTTP API until SIGINT/SIGTERM.
func InitiateAPI() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, logger, shutdownTelemetry, err := bootstrap(ctx)
	if err != nil {
		return err
	}
	defer logger.Sync() //nolint:errcheck

	api, err := BuildAPI(ctx, cfg, logger)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              net.JoinHostPort(cfg.Server.Host, strconv.Itoa(cfg.Server.Port)),
		Handler:           api.Handler,
		ReadTimeout:       cfg.Server.ReadTimeout,
		ReadHeaderTimeout: cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		logger.Info("http server listening", zap.String("addr", srv.Addr), zap.String("docs", "/docs"))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})
	pprofSrv := startPprof(g, cfg, logger)

	<-gctx.Done()
	logger.Info("shutting down api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	errs := []error{srv.Shutdown(shutdownCtx)}
	if pprofSrv != nil {
		errs = append(errs, pprofSrv.Shutdown(shutdownCtx))
	}
	errs = append(errs, g.Wait(), api.Platform.Close(shutdownCtx), shutdownTelemetry(shutdownCtx))
	return errors.Join(errs...)
}

// Worker bundles the audit consumer and the clients it needs. Exported so
// integration tests can run the real worker in-process against testcontainers.
type Worker struct {
	Platform *Platform
	Modules  Modules
	client   *kgo.Client
	consumer *event.AuditConsumer
}

// BuildWorker connects the platform and a consumer-group client for every
// configured domain topic.
func BuildWorker(ctx context.Context, cfg *config.Config, logger *zap.Logger) (_ *Worker, err error) {
	p, err := newPlatform(ctx, cfg, logger, workerNeeds)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = p.Close(context.Background())
		}
	}()
	mods := newModules(cfg, p, logger)

	client, err := kafka.NewConsumer(ctx, cfg.Kafka, cfg.Kafka.Topics.List(), kgo.BlockRebalanceOnPoll())
	if err != nil {
		return nil, err
	}
	consumer := event.NewAuditConsumer(client, kafka.Tracer, mods.Audit, event.AuditConsumerConfig{
		BatchSize:       cfg.Audit.BatchSize,
		MaxPollRecords:  cfg.Audit.MaxPollRecords,
		RetryMaxBackoff: cfg.Audit.RetryMaxBackoff,
		MaxWorkers:      cfg.Audit.MaxWorkers,
	}, logger.Named("consumer"))
	return &Worker{Platform: p, Modules: mods, client: client, consumer: consumer}, nil
}

// Run consumes until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	return w.consumer.Run(ctx)
}

// Close stops the consumer client, then the platform clients.
func (w *Worker) Close(ctx context.Context) error {
	w.client.Close()
	return w.Platform.Close(ctx)
}

// InitiateWorker runs the audit consumer until SIGINT/SIGTERM.
func InitiateWorker() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, logger, shutdownTelemetry, err := bootstrap(ctx)
	if err != nil {
		return err
	}
	defer logger.Sync() //nolint:errcheck

	w, err := BuildWorker(ctx, cfg, logger)
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		logger.Info("audit consumer started",
			zap.String("group", cfg.Kafka.ConsumerGroup), zap.Strings("topics", cfg.Kafka.Topics.List()))
		return w.Run(gctx)
	})
	pprofSrv := startPprof(g, cfg, logger)

	<-gctx.Done()
	logger.Info("shutting down worker")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	var errs []error
	if pprofSrv != nil {
		errs = append(errs, pprofSrv.Shutdown(shutdownCtx))
	}
	errs = append(errs, g.Wait(), w.Close(shutdownCtx), shutdownTelemetry(shutdownCtx))
	return errors.Join(errs...)
}

func bootstrap(ctx context.Context) (*config.Config, *zap.Logger, func(context.Context) error, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, nil, nil, err
	}
	logger, err := NewLogger(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	shutdown, err := initTelemetry(ctx, cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	return cfg, logger, shutdown, nil
}

// startPprof serves net/http/pprof on a separate admin port when configured.
// It is never exposed on the public API listener.
func startPprof(g *errgroup.Group, cfg *config.Config, logger *zap.Logger) *http.Server {
	if cfg.Server.PprofPort == 0 {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{
		Addr:              net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.Server.PprofPort)),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	g.Go(func() error {
		logger.Info("pprof listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("pprof server: %w", err)
		}
		return nil
	})
	return srv
}
