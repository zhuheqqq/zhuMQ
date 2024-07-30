package clients

import (
	"context"
	"errors"
	"sync"
	"zhuMQ/kitex_gen/api"
	"zhuMQ/kitex_gen/api/server_operations"
)

type Producer struct {
	Cli            server_operations.Client
	rmu            sync.RWMutex
	Name           string
	Topic_Partions map[string]bool //表示该Topic的分片是否是这个producer负责
}

type Message struct {
	Topic_name string
	Part_name  string
	Msg        string
}

func (p *Producer) Push(msg Message) error {

	resp, err := p.Cli.Push(context.Background(), &api.PushRequest{
		Producer: p.Name,
		Topic:    msg.Topic_name,
		Key:      msg.Part_name,
		Message:  msg.Msg,
	})
	if err == nil && resp.Ret {
		return nil
	} else {
		return errors.New("err != nil or resp.Ret == false")
	}

}
