// Package router is the HTTP inbound adapter. It registers Huma operations
// (which also produce the OpenAPI document), converts DTOs to domain models,
// calls the module (core), and maps application errors to HTTP responses. It
// must not import internal/storage.
package router

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"go.uber.org/zap"

	"github.com/hunderaweke/dps-audit-service/internal/const/dto"
	"github.com/hunderaweke/dps-audit-service/internal/const/models"
	"github.com/hunderaweke/dps-audit-service/internal/module"
)

type example struct {
	module module.Example
	errs   errorMapper
}

func RegisterExample(api huma.API, m module.Example, logger *zap.Logger) {
	h := &example{module: m, errs: errorMapper{logger: logger}}
	tags := []string{"Examples"}

	huma.Register(api, huma.Operation{
		OperationID:   "create-example",
		Method:        http.MethodPost,
		Path:          "/v1/examples",
		Summary:       "Create an example",
		Tags:          tags,
		DefaultStatus: http.StatusCreated,
	}, h.create)

	huma.Register(api, huma.Operation{
		OperationID: "get-example",
		Method:      http.MethodGet,
		Path:        "/v1/examples/{id}",
		Summary:     "Get an example by ID",
		Tags:        tags,
	}, h.get)

	huma.Register(api, huma.Operation{
		OperationID: "list-examples",
		Method:      http.MethodGet,
		Path:        "/v1/examples",
		Summary:     "List examples",
		Tags:        tags,
	}, h.list)
}

func (h *example) create(ctx context.Context, in *dto.CreateExampleRequest) (*dto.ExampleResponse, error) {
	ex, err := h.module.Create(ctx, in.Body.ToModel())
	if err != nil {
		return nil, h.errs.toHuma(ctx, err)
	}
	return &dto.ExampleResponse{Body: dto.ExampleFromModel(ex)}, nil
}

func (h *example) get(ctx context.Context, in *dto.GetExampleRequest) (*dto.ExampleResponse, error) {
	ex, err := h.module.Get(ctx, in.ID)
	if err != nil {
		return nil, h.errs.toHuma(ctx, err)
	}
	return &dto.ExampleResponse{Body: dto.ExampleFromModel(ex)}, nil
}

func (h *example) list(ctx context.Context, in *dto.ListExamplesRequest) (*dto.ListExamplesResponse, error) {
	items, err := h.module.List(ctx, models.Page{Limit: in.Limit, Offset: in.Offset})
	if err != nil {
		return nil, h.errs.toHuma(ctx, err)
	}
	resp := &dto.ListExamplesResponse{}
	resp.Body.Items = make([]dto.Example, 0, len(items))
	for _, it := range items {
		resp.Body.Items = append(resp.Body.Items, dto.ExampleFromModel(it))
	}
	resp.Body.Limit, resp.Body.Offset = in.Limit, in.Offset
	return resp, nil
}
