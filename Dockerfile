# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS builder

WORKDIR /app

ENV CGO_ENABLED=0 GOOS=linux

# No `go mod download`: go.mod also pins dev tools (golangci-lint, buf, ...),
# whose dependencies the binaries don't need. `go build` fetches only what is
# imported, and the cache mounts keep it fast across builds.
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker && \
    go build -trimpath -ldflags="-s -w" -o /out/account-stub ./tests/stubs/account

# Shared runtime base: CA certs + default config, non-root user.
# Migrations are embedded in the binary.
FROM scratch AS runtime
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY config/config.yaml /config/config.yaml
ENV CONFIG_PATH=/config/config.yaml
USER 65534:65534

FROM runtime AS api
COPY --from=builder /out/api /bin/api
EXPOSE 8080
ENTRYPOINT ["/bin/api"]

FROM runtime AS worker
COPY --from=builder /out/worker /bin/worker
ENTRYPOINT ["/bin/worker"]

# Local-dev only: stub of the external account gRPC service.
FROM runtime AS account-stub
COPY --from=builder /out/account-stub /bin/account-stub
EXPOSE 9090
ENTRYPOINT ["/bin/account-stub"]
