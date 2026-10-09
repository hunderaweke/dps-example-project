package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cucumber/godog"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// steps holds per-scenario state. A fresh instance is created per scenario.
type steps struct {
	env       env
	http      *http.Client
	status    int
	body      []byte
	createdID string
}

func newSteps(e env) *steps {
	return &steps{env: e, http: &http.Client{Timeout: 10 * time.Second}}
}

func (s *steps) register(sc *godog.ScenarioContext) {
	sc.Step(`^I create an example with name "([^"]*)" and owner "([^"]*)"$`, s.createExample)
	sc.Step(`^I fetch the created example$`, s.fetchCreated)
	sc.Step(`^I fetch an example with a random id$`, s.fetchRandom)
	sc.Step(`^I list examples$`, s.list)
	sc.Step(`^the response status should be (\d+)$`, s.statusShouldBe)
	sc.Step(`^the response field "([^"]*)" should be "([^"]*)"$`, s.fieldShouldBe)
	sc.Step(`^the list should contain at least (\d+) items?$`, s.listAtLeast)
	sc.Step(`^an example.created event should be published for the created example$`, s.eventPublished)
}

func (s *steps) do(method, path string, body any) error {
	var r io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, s.env.baseURL+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	s.status = resp.StatusCode
	s.body, err = io.ReadAll(resp.Body)
	return err
}

func (s *steps) createExample(name, owner string) error {
	if err := s.do(http.MethodPost, "/v1/examples", map[string]string{"name": name, "owner_id": owner}); err != nil {
		return err
	}
	if s.status == http.StatusCreated {
		var out struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(s.body, &out); err != nil {
			return err
		}
		s.createdID = out.ID
	}
	return nil
}

func (s *steps) fetchCreated() error {
	if s.createdID == "" {
		return fmt.Errorf("no example was created in this scenario")
	}
	return s.do(http.MethodGet, "/v1/examples/"+s.createdID, nil)
}

func (s *steps) fetchRandom() error {
	return s.do(http.MethodGet, "/v1/examples/"+uuid.NewString(), nil)
}

func (s *steps) list() error {
	return s.do(http.MethodGet, "/v1/examples?limit=50", nil)
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

func (s *steps) listAtLeast(n int) error {
	var out struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(s.body, &out); err != nil {
		return err
	}
	if len(out.Items) < n {
		return fmt.Errorf("expected at least %d items, got %d", n, len(out.Items))
	}
	return nil
}

func (s *steps) eventPublished() error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(s.env.brokers...),
		kgo.ConsumeTopics(exampleCreatedTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		var found bool
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Key) == s.createdID {
				found = true
			}
		})
		if found {
			return nil
		}
	}
	return fmt.Errorf("no %s event for example %s", exampleCreatedTopic, s.createdID)
}
