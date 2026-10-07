// Package e2e runs the Gherkin features in ./features against the real API
// (built by initiator.BuildAPI) served in-process, with Postgres started by
// testcontainers. Requires Docker; skipped with -short.
package e2e_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"

	"github.com/hunderaweke/dps-audit-service/config"
	"github.com/hunderaweke/dps-audit-service/initiator"
)

type env struct {
	baseURL string
	pgURL   string
}

func TestFeatures(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e tests require docker")
	}
	e := setup(t)

	suite := godog.TestSuite{
		Name:                "dps-audit-service",
		ScenarioInitializer: func(sc *godog.ScenarioContext) { newSteps(e).register(sc) },
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			TestingT: t,
			Strict:   true,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("feature tests failed")
	}
}

func setup(t *testing.T) env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // first run pulls images
	defer cancel()

	pg, err := tcpostgres.Run(ctx, "postgres:17-alpine", tcpostgres.WithDatabase("audit"), tcpostgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, pg)
	must(t, err)
	pgURL, err := pg.ConnectionString(ctx, "sslmode=disable")
	must(t, err)

	cfg := &config.Config{
		App:      config.App{Name: "dps-audit-service", Environment: "test", Version: "e2e"},
		Server:   config.Server{CORSOrigins: []string{"*"}},
		Postgres: config.Postgres{URL: pgURL, MaxConns: 5, AutoMigrate: true},
	}

	api, err := initiator.BuildAPI(ctx, cfg, zap.NewNop())
	must(t, err)
	srv := httptest.NewServer(api.Handler)
	t.Cleanup(func() {
		srv.Close()
		_ = api.Platform.Close(context.Background())
	})
	return env{baseURL: srv.URL, pgURL: pgURL}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
