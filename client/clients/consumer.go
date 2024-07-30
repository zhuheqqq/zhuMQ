package clients

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cloudwego/kitex/server"
	"net"
	"sync"
	"zhuMQ/kitex_gen/api"
	ser "zhuMQ/kitex_gen/api/client_operations"
	"zhuMQ/kitex_gen/api/server_operations"
)

type Consumer struct {
	Cli  server_operations.Client //连接多个broker
	Name string
	srv  server.Server

	State string
	mu    sync.RWMutex
}

type Info struct {
	offset int64
	topic  string
	part   string
	option int8
	bufs   map[int64]*api.PubRequest
}

func NewConsumer() Consumer {
	return Consumer{
		mu:    sync.RWMutex{},
		State: "alive",
	}
}

func (c *Consumer) Alive() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.State
}

func (c *Consumer) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	fmt.Println(req)

	return &api.PubResponse{
		Ret: true,
	}, nil
}

func (c *Consumer) Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	fmt.Println("Pingpong")
	return &api.PingPongResponse{Pong: true}, nil
}

func (c *Consumer) Start_server(port string) {
	addr, _ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))

	svr := ser.NewServer(c, opts...)
	c.srv = svr
	err := svr.Run()
	if err != nil {
		println(err.Error())
	}
}

func (c *Consumer) ShutDown_server() {
	c.srv.Stop()
}

func (c *Consumer) Down() {
	c.mu.Lock()
	c.State = "down"
	c.mu.Unlock()
}

func (c *Consumer) SubScription(sub api.SubRequest) (ret []PartKey, err error) {
	//查询Zookeeper，找到broker

	//发送RPC请求
	resp, err := c.Cli.Sub(context.Background(), &sub)
	if err != nil || !resp.Ret {
		return ret, err
	}

	parts := make([]PartKey, resp.Size)

	json.Unmarshal(resp.Parts, &parts)

	return parts, nil
}

func (c *Consumer) StartGet(info Info) (err error) {
	ret := ""
	req := api.InfoGetRequest{
		CliName:   c.Name,
		TopicName: info.topic,
		PartName:  info.part,
		Offset:    info.offset,
		Option:    info.option,
	}
	resp, err := c.Cli.StarttoGet(context.Background(), &req)
	if err != nil || !resp.Ret {
		ret += info.topic + info.part + ": err != nil or resp.Ret == false\n"
	}
	if ret == "" {
		return nil
	} else {
		return errors.New(ret)
	}
}
