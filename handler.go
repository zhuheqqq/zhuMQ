package main

import (
	"context"
	"zhuMQ/kitex_gen/api"
)

type Client_OperationsImpl struct{}

func (s *Client_OperationsImpl) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	return &api.PubResponse{Ret: true}, nil
}

func (s *Client_OperationsImpl) Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	return &api.PingPongResponse{
		Pong: true,
	}, err
}
