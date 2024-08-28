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
	Cli             server_operations.Client
	rmu             sync.RWMutex
	Name            string
	Topic_Partions  map[string]server_operations.Client //表示该Topic的分片是否是这个producer负责
	ZkBroker        zkserver_operations.Client          //zookeeper客户端，用于获取分片的broker信息
	Top_Part_indexs map[string]int64
}

type Message struct {
	Topic_name string
	Part_name  string
	Msg        []byte
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

func (p *Producer) Push(msg Message, ack int) error {
	str := msg.Topic_name + msg.Part_name
	var ok2 bool
	var index int64
	p.rmu.RLock()
	cli, ok1 := p.Topic_Partions[str]
	if ack == -1 { //raft, 获取index
		index, ok2 = p.Top_Part_indexs[str]
	}
	zk := p.ZkBroker
	p.rmu.RUnlock()

	if !ok1 {
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
		p.Topic_Partions[str] = cli
		p.rmu.Unlock()
	}

	if ack == -1 {
		if !ok2 {
			p.rmu.Lock()
			p.Top_Part_indexs[str] = 0
			p.rmu.Unlock()
		}
	}

	//若partition所在的broker发生改变，将返回信息，重新请求zkserver
	resp, err := cli.Push(context.Background(), &api.PushRequest{
		Producer: p.Name,
		Topic:    msg.Topic_name,
		Key:      msg.Part_name,
		Message:  msg.Msg,
		Ack:      int8(ack),
		Cmdindex: index,
	})
	if err == nil && resp.Ret {
		p.rmu.Lock()
		p.Top_Part_indexs[str] = index + 1
		p.rmu.Unlock()

		return nil
	} else if resp.Err == "partition remove" {
		p.rmu.Lock()
		delete(p.Topic_Partions, str)
		p.rmu.Unlock()

		return p.Push(msg, ack) //重新发送该信息

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
		rmu:             sync.RWMutex{},
		Name:            name,
		Topic_Partions:  make(map[string]server_operations.Client),
		Top_Part_indexs: make(map[string]int64),
	}
	var err error
	P.ZkBroker, err = zkserver_operations.NewClient(P.Name, client.WithHostPorts(zkbroker))

	return &P, err
}
