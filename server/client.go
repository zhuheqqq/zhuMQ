package server

import (
	"context"
	"sync"
	"time"
	"zhuMQ/kitex_gen/api"
	"zhuMQ/kitex_gen/api/client_operations"
)

const (
	ALIVE = "alive"
	DONE  = "done"
)

type Client struct {
	mu       sync.RWMutex
	name     string
	consumer client_operations.Client
	subList  []*SubScription //客户端订阅列表
	state    string          //客户端状态
}

// 一个主题的消费组
type Group struct {
	rmu        sync.RWMutex
	topic_name string //主题名称
	consumers  map[string]bool
}

func NewClient(ipport string, con client_operations.Client) *Client {
	client := &Client{
		mu:       sync.RWMutex{},
		name:     ipport,
		consumer: con,
		state:    ALIVE,
		subList:  make([]*SubScription, 0),
	}
	return client
}

// 创建新的Group实例并将一个消费者添加到组中
func NewGroup(topic_name, cli_name string) *Group {
	group := &Group{
		rmu:        sync.RWMutex{},
		topic_name: topic_name,
	}
	group.consumers[cli_name] = true
	return group
}

// 将一个消费者添加到组中
func (g *Group) AddClient(cli_name string) {
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		g.consumers[cli_name] = true
	}
	g.rmu.Unlock()
}

// 将消费者标记为不活跃
func (g *Group) DownClient(cli_name string) {
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		g.consumers[cli_name] = false
	}
	g.rmu.Unlock()
}

func (g *Group) DeleteClient(cli_name string) {
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		delete(g.consumers, cli_name)
	}
	g.rmu.Unlock()
}

// 不断发送Pingpong请求检查消费者是否活跃
func (c *Client) CheckConsumer() bool {
	c.mu = sync.RWMutex{}
	for {
		resp, err := c.consumer.Pingpong(context.Background(), &api.PingPongRequest{Ping: true})
		if err != nil || !resp.Pong {
			break
		}
		time.Sleep(time.Second)
	}
	c.mu.Lock()
	c.state = DONE
	c.mu.Unlock()
	return true
}

// 向客户端的订阅列表中添加新的订阅
func (c *Client) AddSubScription(sub *SubScription) {
	c.mu.Lock()
	c.subList = append(c.subList, sub)
	c.mu.Unlock()
}

// 向服务器发送消息
func (c *Client) Pub(message string) bool {
	resp, err := c.consumer.Pub(context.Background(), &api.PubRequest{
		Meg: message,
	})
	if err != nil || !resp.Ret {
		return false
	}
	return true
}
