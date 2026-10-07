package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cucumber/godog"
	"github.com/jackc/pgx/v5"
)

// steps holds per-scenario state. A fresh instance is created per scenario.
type steps struct {
	env    env
	http   *http.Client
	status int
	body   []byte
}

func newSteps(e env) *steps {
	return &steps{env: e, http: &http.Client{Timeout: 10 * time.Second}}
}

func (s *steps) register(sc *godog.ScenarioContext) {
	sc.Step(`^I request "([^"]*)" "([^"]*)"$`, s.do)
	sc.Step(`^the response status should be (\d+)$`, s.statusShouldBe)
	sc.Step(`^the response field "([^"]*)" should be "([^"]*)"$`, s.fieldShouldBe)
	sc.Step(`^the readiness check "([^"]*)" should be "([^"]*)"$`, s.checkShouldBe)
	sc.Step(`^the "([^"]*)" table should exist$`, s.tableExists)
}

func (s *steps) do(method, path string) error {
	req, err := http.NewRequest(method, s.env.baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	s.status = resp.StatusCode
	s.body, err = io.ReadAll(resp.Body)
	return err
}

func (s *steps) statusShouldBe(want int) error {
	if s.status != want {
		return fmt.Errorf("expected status %d, got %d: %s", want, s.status, s.body)
	}
	return nil
}

func (s *steps) fieldShouldBe(field, want string) error {
	var out map[string]any
	if err := json.Unmarshal(s.body, &out); err != nil {
		return err
	}
	if got := fmt.Sprint(out[field]); got != want {
		return fmt.Errorf("expected %s=%q, got %q", field, want, got)
	}
	return nil
}

func (s *steps) checkShouldBe(name, want string) error {
	var out struct {
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(s.body, &out); err != nil {
		return err
	}
	if got := out.Checks[name]; got != want {
		return fmt.Errorf("expected check %s=%q, got %q (%s)", name, want, got, s.body)
	}
	return nil
}

func (s *steps) tableExists(table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, s.env.pgURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("table %s does not exist", table)
	}
	return nil
}
