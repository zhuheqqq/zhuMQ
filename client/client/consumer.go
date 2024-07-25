package client

import (
	"context"
	"fmt"
	"github.com/cloudwego/kitex/server"
	"net"
	"zhuMQ/kitex_gen/api"
	"zhuMQ/kitex_gen/api/client_operations"
	ser "zhuMQ/kitex_gen/api/client_operations"
)

type Consumer struct {
	cli client_operations.Client
}

func (c *Consumer) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	fmt.Println(req.Meg)
	return &api.PubResponse{
		Ret: true,
	}, nil
}

func (c *Consumer) Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	return &api.PingPongResponse{Pong: true}, nil
}

func (c *Consumer) start_server(port string) {
	addr, _ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))

	svr := ser.NewServer(new(Consumer), opts...)
	err := svr.Run()
	if err != nil {
		println(err.Error())
	}
}
