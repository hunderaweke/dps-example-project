package router

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/hunderaweke/dps-audit-service/internal/const/dto"
)

// Check reports whether one dependency is reachable.
type Check func(ctx context.Context) error

// RegisterHealth adds /healthz (liveness: the process is up) and /readyz
// (readiness: every dependency check passes).
func RegisterHealth(api huma.API, checks map[string]Check) {
	tags := []string{"Health"}

	huma.Register(api, huma.Operation{
		OperationID: "healthz", Method: http.MethodGet, Path: "/healthz", Summary: "Liveness probe", Tags: tags,
	}, func(ctx context.Context, _ *struct{}) (*dto.HealthResponse, error) {
		resp := &dto.HealthResponse{}
		resp.Body.Status = "ok"
		return resp, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "readyz", Method: http.MethodGet, Path: "/readyz", Summary: "Readiness probe", Tags: tags,
	}, func(ctx context.Context, _ *struct{}) (*dto.HealthResponse, error) {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		var (
			mu      sync.Mutex
			wg      sync.WaitGroup
			results = make(map[string]string, len(checks))
			healthy = true
		)
		for name, check := range checks {
			wg.Go(func() {
				status := "ok"
				if err := check(ctx); err != nil {
					status = err.Error()
				}
				mu.Lock()
				defer mu.Unlock()
				results[name] = status
				healthy = healthy && status == "ok"
			})
		}
		wg.Wait()

		if !healthy {
			return nil, huma.NewError(http.StatusServiceUnavailable, "not ready", &huma.ErrorDetail{Message: "dependency check failed", Value: results})
		}
		resp := &dto.HealthResponse{}
		resp.Body.Status = "ok"
		resp.Body.Checks = results
		return resp, nil
	})
}
