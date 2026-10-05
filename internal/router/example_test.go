package router_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	apperrors "github.com/hunderaweke/dps-audit-service/internal/const/errors"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
	"github.com/hunderaweke/dps-audit-service/internal/module/mocks"
	"github.com/hunderaweke/dps-audit-service/internal/router"
)

func newServer(m module.Example) http.Handler {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	api := humagin.New(engine, huma.DefaultConfig("test", "0.0.0"))
	router.RegisterExample(api, m, zap.NewNop())
	return engine
}

func do(h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateExample(t *testing.T) {
	t.Run("201 on success", func(t *testing.T) {
		m := mocks.NewExample(t)
		m.EXPECT().Create(mock.Anything, models.Example{Name: "first", OwnerID: "acc_1"}).
			Return(models.Example{ID: uuid.New(), Name: "first", OwnerID: "acc_1", Status: models.ExampleStatusPending}, nil)

		rec := do(newServer(m), http.MethodPost, "/v1/examples", map[string]string{"name": "first", "owner_id": "acc_1"})
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

		var got map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		assert.Equal(t, "pending", got["status"])
	})

	t.Run("422 from Huma validation never reaches the module", func(t *testing.T) {
		m := mocks.NewExample(t) // no expectations: any call fails the test
		rec := do(newServer(m), http.MethodPost, "/v1/examples", map[string]string{"name": "x"})
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	})
}

func TestGetExample(t *testing.T) {
	id := uuid.New()

	t.Run("404 maps from ErrNotFound", func(t *testing.T) {
		m := mocks.NewExample(t)
		m.EXPECT().Get(mock.Anything, id).Return(models.Example{}, apperrors.ErrNotFound.New("example not found"))

		rec := do(newServer(m), http.MethodGet, "/v1/examples/"+id.String(), nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("500 hides internal details", func(t *testing.T) {
		m := mocks.NewExample(t)
		m.EXPECT().Get(mock.Anything, id).Return(models.Example{}, apperrors.ErrDBRead.New("connection refused to 10.0.0.1"))

		rec := do(newServer(m), http.MethodGet, "/v1/examples/"+id.String(), nil)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.NotContains(t, rec.Body.String(), "10.0.0.1")
	})
}

// stubExample is a zero-overhead module used by benchmarks so they measure the
// HTTP layer (routing, decoding, validation, encoding), not mock bookkeeping.
type stubExample struct{ module.Example }

func (stubExample) Create(_ context.Context, ex models.Example) (models.Example, error) {
	ex.ID, ex.Status = uuid.New(), models.ExampleStatusPending
	return ex, nil
}

func (stubExample) Get(_ context.Context, id uuid.UUID) (models.Example, error) {
	return models.Example{ID: id, Name: "bench", OwnerID: "acc_1", Status: models.ExampleStatusPending}, nil
}

// stubPage is built once so BenchmarkListExamples measures encoding, not setup.
var stubPage = func() []models.Example {
	items := make([]models.Example, 100)
	for i := range items {
		items[i] = models.Example{ID: uuid.New(), Name: "bench", OwnerID: "acc_1", Status: models.ExampleStatusPending}
	}
	return items
}()

func (stubExample) List(_ context.Context, page models.Page) ([]models.Example, error) {
	return stubPage[:min(int(page.Limit), len(stubPage))], nil
}
