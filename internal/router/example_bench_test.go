package router_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

// BenchmarkCreateExample measures the HTTP adapter overhead: gin routing,
// Huma decoding + schema validation, DTO mapping and JSON encoding.
func BenchmarkCreateExample(b *testing.B) {
	h := newServer(stubExample{})
	body := []byte(`{"name":"benchmark","owner_id":"acc_1","description":"load"}`)
	b.ReportAllocs()

	for b.Loop() {
		req := httptest.NewRequest(http.MethodPost, "/v1/examples", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkGetExample(b *testing.B) {
	h := newServer(stubExample{})
	path := "/v1/examples/" + uuid.NewString()
	b.ReportAllocs()

	for b.Loop() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d", rec.Code)
		}
	}
}
