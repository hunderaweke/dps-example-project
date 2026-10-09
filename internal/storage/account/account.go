package account

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apperrors "github.com/username/example-service/internal/const/errors"
	"github.com/username/example-service/internal/const/models"
	"github.com/username/example-service/internal/module"
	sdk "github.com/username/example-service/pkg/account"
)

type client struct {
	sdk *sdk.Client
}

var _ module.AccountClient = (*client)(nil)

func New(c *sdk.Client) module.AccountClient {
	return &client{sdk: c}
}

func (c *client) GetAccount(ctx context.Context, id string) (models.Account, error) {
	acc, err := c.sdk.GetAccount(ctx, id)
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			return models.Account{}, apperrors.ErrNotFound.New("account %s not found", id)
		case codes.Unavailable, codes.DeadlineExceeded:
			return models.Account{}, apperrors.ErrUnavailable.Wrap(err, "account service")
		default:
			return models.Account{}, apperrors.ErrInternal.Wrap(err, "get account %s", id)
		}
	}
	return models.Account{ID: acc.ID, Name: acc.Name, Active: acc.Active}, nil
}
