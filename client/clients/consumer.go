package clients

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
	"net"
	"sync"
	"zhuMQ/kitex_gen/api"
	ser "zhuMQ/kitex_gen/api/client_operations"
	"zhuMQ/kitex_gen/api/server_operations"
	"zhuMQ/kitex_gen/api/zkserver_operations"
)

type Consumer struct {
	Name string
	srv  server.Server

	State    string
	mu       sync.RWMutex
	port     string
	zkBroker zkserver_operations.Client
	Brokers  map[string]*server_operations.Client
}

func NewConsumer(zkbroker string, name string, port string) (*Consumer, error) {
	C := Consumer{
		mu:      sync.RWMutex{},
		Name:    name,
		State:   "alive",
		port:    port,
		Brokers: make(map[string]*server_operations.Client),
	}

	var err error
	C.zkBroker, err = zkserver_operations.NewClient(C.Name, client.WithHostPorts(zkbroker))

	return &C, err
}

func (c *Consumer) Alive() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.State
}

// 未实现
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

func (c *Consumer) Start_server() {
	addr, _ := net.ResolveTCPAddr("tcp", c.port)
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

// 标记消费者不可用
func (c *Consumer) Down() {
	c.mu.Lock()
	c.State = "down"
	c.mu.Unlock()
}

func (c *Consumer) SubScription(topic, partition string, option int8) (clis []*server_operations.Client, err error) {
	//查询Zookeeper，找到broker
	c.mu.RLock()
	zk := c.zkBroker
	c.mu.RUnlock()

	resp, err := zk.ConGetBroker(context.Background(), &api.ConGetBrokRequest{
		TopicName: topic,
		PartName:  partition,
		Option:    option,
	})
	if err != nil || !resp.Ret {
		return nil, err
	}

	broks := make([]BrokerInfo, resp.Size)
	json.Unmarshal(resp.Broks, &broks)

	parts := make([]PartKey, resp.Size) //not used
	json.Unmarshal(resp.Parts, &parts)  //not used

	//发送Sub请求
	for _, brok := range broks {
		cli, err := server_operations.NewClient(c.Name, client.WithHostPorts(brok.Host_port))
		if err != nil || !resp.Ret {
			return clis, err
		}
		clis = append(clis, &cli)
		c.Brokers[brok.Name] = &cli

		c.SendInfo(c.port, &cli)

		resp, err := cli.Sub(context.Background(), &api.SubRequest{
			Consumer: c.Name,
			Topic:    topic,
			Key:      partition,
			Option:   option,
		})
		if err != nil || !resp.Ret {
			return clis, err
		}
	}

	return clis, nil
}

// 向broker发送开始获取消息的请求
func (c *Consumer) SendInfo(port string, cli *server_operations.Client) error {

	resp, err := (*cli).ConInfo(context.Background(), &api.InfoRequest{
		IpPort: port,
	})
	if err != nil {
		fmt.Println(resp)
	}
	return err
}

func (c *Consumer) StartGet(info Info) (err error) {
	ret := ""
	req := api.InfoGetRequest{
		CliName:   c.Name,
		TopicName: info.Topic,
		PartName:  info.Part,
		Offset:    info.Offset,
		Option:    info.Option,
	}
	resp, err := info.Cli.StarttoGet(context.Background(), &req)
	if err != nil || !resp.Ret {
		ret += info.Topic + info.Part + ": err != nil or resp.Ret == false\n"
	}
	if ret == "" {
		return nil
	} else {
		return errors.New(ret)
	}
}

type Info struct {
	Offset int64
	Topic  string
	Part   string
	Option int8
	Bufs   map[int64]*api.PubRequest
	Cli    server_operations.Client
}

func NewInfo(offset int64, topic, part string) Info {
	return Info{
		Offset: offset,
		Topic:  topic,
		Part:   part,
		Bufs:   make(map[int64]*api.PubRequest),
	}
}
