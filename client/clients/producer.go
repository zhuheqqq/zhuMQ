package clients

import (
	"context"
	"errors"
	"github.com/cloudwego/kitex/client"
	"sync"
	"zhuMQ/kitex_gen/api"
	"zhuMQ/kitex_gen/api/server_operations"
	"zhuMQ/kitex_gen/api/zkserver_operations"
)

type Producer struct {
	Cli            server_operations.Client
	rmu            sync.RWMutex
	Name           string
	Topic_Partions map[string]server_operations.Client //表示该Topic的分片是否是这个producer负责
	ZkBroker       zkserver_operations.Client          //zookeeper客户端，用于获取分片的broker信息
}

type Message struct {
	Topic_name string
	Part_name  string
	Msg        string
}

func (p *Producer) SetPartitionState(topic_name, part_name string, option, dupnum int8) error {
	resp, err := p.ZkBroker.SetPartitionState(context.Background(), &api.SetPartitionStateRequest{
		Topic:     topic_name,
		Partition: part_name,
		Option:    option,
		Dupnum:    dupnum,
	})

	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

func (p *Producer) Push(msg Message) error {
	index := msg.Topic_name + msg.Part_name //组合topic_name和part_name作为键

	p.rmu.RLock()
	cli, ok := p.Topic_Partions[index]
	zk := p.ZkBroker
	p.rmu.RUnlock()

	if !ok {
		resp, err := zk.ProGetBroker(context.Background(), &api.ProGetBrokRequest{
			TopicName: msg.Topic_name,
			PartName:  msg.Part_name,
		})

		if err != nil || !resp.Ret {
			return err
		}

		cli, err = server_operations.NewClient(p.Name, client.WithHostPorts(resp.BrokerHostPort))

		if err != nil {
			return err
		}

		p.rmu.Lock()
		p.Topic_Partions[index] = cli
		p.rmu.Unlock()
	}

	resp, err := cli.Push(context.Background(), &api.PushRequest{
		Producer: p.Name,
		Topic:    msg.Topic_name,
		Key:      msg.Part_name,
		Message:  msg.Msg,
	})
	if err == nil && resp.Ret {
		return nil
	} else if resp.Err == "partition remove" {
		p.rmu.Lock()
		delete(p.Topic_Partions, index)
		p.rmu.Unlock()

		return p.Push(msg) //重新发送该信息

	} else {
		return errors.New("err != " + err.Error() + "or resp.Ret == false")
	}
}

func (p *Producer) CreateTopic(topic_name string) error {

	resp, err := p.ZkBroker.CreateTopic(context.Background(), &api.CreateTopicRequest{
		TopicName: topic_name,
	})

	if err != nil || !resp.Ret {
		return err
	}

	return nil
}

func (p *Producer) CreatePart(topic_name, part_name string) error {
	resp, err := p.ZkBroker.CreatePart(context.Background(), &api.CreatePartRequest{
		TopicName: topic_name,
		PartName:  part_name,
	})

	if err != nil || !resp.Ret {
		return err
	}

	return nil
}

func NewProducer(zkbroker string, name string) (*Producer, error) {
	P := Producer{
		rmu:            sync.RWMutex{},
		Name:           name,
		Topic_Partions: make(map[string]server_operations.Client),
	}
	var err error
	P.ZkBroker, err = zkserver_operations.NewClient(P.Name, client.WithHostPorts(zkbroker))

	return &P, err
}
