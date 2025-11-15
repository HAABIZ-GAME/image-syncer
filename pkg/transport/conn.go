package transport

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewConn(ctx context.Context, target string) (*grpc.ClientConn, error) {
	if len(target) == 0 {
		return nil, errors.New("target is null, it should be a remote endpoint or a unix domain socket")
	}
	conn, err := grpc.DialContext(ctx, target, grpc.WithCredentialsBundle(insecure.NewBundle()), grpc.WithBlock())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial %s", target)
	}

	return conn, nil
}
