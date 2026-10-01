package account

import (
	"context"
	"time"

	"google.golang.org/grpc"

	accountv1 "github.com/username/example-service/pkg/account/gen/account/v1"
)

type Account struct {
	ID     string
	Name   string
	Active bool
}

type Client struct {
	rpc     accountv1.AccountServiceClient
	timeout time.Duration
}

func NewClient(conn grpc.ClientConnInterface, timeout time.Duration) *Client {
	return &Client{rpc: accountv1.NewAccountServiceClient(conn), timeout: timeout}
}

// GetAccount returns the raw gRPC error so callers can inspect status codes.
func (c *Client) GetAccount(ctx context.Context, id string) (Account, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	resp, err := c.rpc.GetAccount(ctx, &accountv1.GetAccountRequest{Id: id})
	if err != nil {
		return Account{}, err
	}
	return Account{ID: resp.GetId(), Name: resp.GetName(), Active: resp.GetActive()}, nil
}
