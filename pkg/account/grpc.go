// Package account is a reusable gRPC client SDK for the account service. It
// lives in pkg/ because it has no dependency on this service's internals and
// could be imported by other services. Its types are converted to domain
// models by internal/storage/account.
package account

import (
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial opens a lazily-connected gRPC client connection. Pass extra options
// (e.g. TLS credentials) to override the insecure default.
func Dial(address string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	defaults := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}
	conn, err := grpc.NewClient(address, append(defaults, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("dial account service %s: %w", address, err)
	}
	return conn, nil
}
