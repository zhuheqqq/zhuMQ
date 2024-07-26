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
	Topic_Partions map[string]bool
}

type Message struct {
	Topic_name string
	Part_name  string
	Msg        string
}

func (p *Producer) Push(msg Message) error {
	index := msg.Topic_name + msg.Part_name
	p.rmu.RLock()
	_, ok := p.Topic_Partions[index]
	p.rmu.RUnlock()

	if ok {
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
	return errors.New("this toipc_part do not in this producer")
}
