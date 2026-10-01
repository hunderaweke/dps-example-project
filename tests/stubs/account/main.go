// Command account is a stub of the external account gRPC service for local
// development and compose. It is not part of the service itself.
//
//	missing_*  -> NotFound
//	inactive_* -> active=false
//	anything else -> active=true
package main

import (
	"context"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountv1 "github.com/username/example-service/pkg/account/gen/account/v1"
)

type server struct {
	accountv1.UnimplementedAccountServiceServer
}

func (server) GetAccount(_ context.Context, req *accountv1.GetAccountRequest) (*accountv1.GetAccountResponse, error) {
	id := req.GetId()
	if strings.HasPrefix(id, "missing_") {
		return nil, status.Errorf(codes.NotFound, "account %s not found", id)
	}
	return &accountv1.GetAccountResponse{
		Id:     id,
		Name:   "Stub " + id,
		Active: !strings.HasPrefix(id, "inactive_"),
	}, nil
}

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":9090"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	accountv1.RegisterAccountServiceServer(s, server{})
	log.Printf("account stub listening on %s", addr)
	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
