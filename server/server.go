package server

import (
	"context"
	"encoding/json"
	"errors"
	client2 "github.com/cloudwego/kitex/client"
	"os"
	"sync"
	"zhuMQ/kitex_gen/api"
	"zhuMQ/kitex_gen/api/client_operations"
	"zhuMQ/kitex_gen/api/zkserver_operations"
	"zhuMQ/zookeeper"
)

var (
	name string
)

const (
	NODE_SIZE = 42
)

// 一个topic包含多个消息分区
type Server struct {
	Name        string
	topics      map[string]*Topic
	consumers   map[string]*Client
	parts_rafts *parts_raft
	zk          zookeeper.ZK
	zkclient    zkserver_operations.Client
	mu          sync.RWMutex
}

type Key struct {
	Start_index int64 `json:"start_index"`
	End_index   int64 `json:"end_index"`
	Size        int   `json:"size"`
}

type Message struct {
	Index      int64  `json:"index"`
	Topic_name string `json:"topic_name"`
	Part_name  string `json:"part_name"`
	Msg        []byte `json:"msg"`
}

type Msg struct {
	producer string
	topic    string
	key      string
	msg      []byte
}

type retpull struct {
	message string
}

type startget struct {
	cli_name   string
	topic_name string
	part_name  string
	index      int64
	option     int8
}

type info struct {
	name       string //broker name
	topic_name string
	part_name  string
	file_name  string
	new_name   string
	option     int8
	offset     int64
	size       int8
	message    string

	producer string
	consumer string
}

//type retsub struct {
//	size  int
//	parts []PartKey
//}

func NewServer(zkinfo zookeeper.ZKInfo) *Server {
	return &Server{
		// topics: make(map[string]*Topic),
		// consumers: make(map[string]*Client),
		zk: *zookeeper.NewZK(zkinfo), //连接上zookeeper
		mu: sync.RWMutex{},
	}
}

// 初始化server实例
func (s *Server) make(opt Options) {
	s.topics = make(map[string]*Topic)
	s.consumers = make(map[string]*Client)

	name = GetIpport()
	s.CheckList()
	s.Name = opt.Name

	//本地创建parts——raft，为raft同步做准备
	s.parts_rafts = NewParts_Raft()
	go s.parts_rafts.make(opt.Name, opt.Raft_Host_Port)

	//连接zkServer，并将自己的Info发送到zkServer,
	zkclient, err := zkserver_operations.NewClient(opt.Name, client.WithHostPorts(opt.Zkserver_Host_Port))
	if err != nil {
		DEBUG(dError, err.Error())
	}
	s.zkclient = zkclient

	resp, err := zkclient.BroInfo(context.Background(), &api.BroInfoRequest{
		BrokerName:     opt.Name,
		BrokerHostPort: opt.Broker_Host_Port,
	})
	if err != nil || !resp.Ret {
		DEBUG(dError, err.Error())
	}
	//s.IntiBroker() 根据zookeeper上的历史信息，加载缓存信息
}

// 获取该Broker需要负责的Topic和Partition,并在本地创建对应配置
func (s *Server) IntiBroker() {
	s.mu.Lock()
	info := Property{
		Name:  s.Name,
		Power: 1,
		//获取Broker性能指标
	}
	data, err := json.Marshal(info)
	if err != nil {
		DEBUG(dError, err.Error())
	}

	resp, err := s.zkclient.BroGetConfig(context.Background(), &api.BroGetConfigRequest{
		Propertyinfo: data,
	})

	if err != nil || !resp.Ret {
		DEBUG(dError, err.Error())
	}
	BroInfo := BroNodeInfo{
		Topics: make(map[string]TopNodeInfo),
	}
	json.Unmarshal(resp.Brokerinfo, &BroInfo)

	s.HandleTopics(BroInfo.Topics)

	s.mu.Unlock()
}

func (s *Server) HandleTopics(Topics map[string]TopNodeInfo) {

	for topic_name, topic := range Topics {
		_, ok := s.topics[topic_name]
		if !ok {
			top := NewTopic(topic_name)
			top.HandleParttitions(topic.Partitions)
			s.topics[topic_name] = top
		} else {
			DEBUG(dWarn, "This topic(%v) had in s.topics\n", topic_name)
		}
	}
}

func (s *Server) StartGet(in info) (err error) {
	/*
		新开启一个consumer关于一个topic和partition的协程来消费该partition的信息；

		查询是否有该订阅的信息；

		PTP：需要负载均衡

		PSB：不需要负载均衡
	*/
	err = nil
	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.mu.RLock()
		defer s.mu.RUnlock()
		//已经由zkserver检查过是否订阅
		sub_name := GetStringfromSub(in.topic_name, in.part_name, in.option)
		//添加到Config后会进行负载均衡，生成新的配置，然后执行新的配置
		return s.topics[in.topic_name].HandleStartToGet(sub_name, in, s.consumers[in.consumer].GetCli())

	case TOPIC_KEY_PSB_PUSH:
		s.mu.RLock()
		defer s.mu.RUnlock()

		sub_name := GetStringfromSub(in.topic_name, in.part_name, in.option)
		// clis := make(map[string]*client_operations.Client)
		// clis[start.cli_name] = s.consumers[start.cli_name].GetCli()
		// file := s.topics[start.topic_name].GetFile(start.part_name)
		// go s.consumers[start.cli_name].StartPart(start, clis, file)
		DEBUG(dLog, "consumer(%v) start to get topic(%v) partition(%v) offset(%v) in sub(%v)\n", in.consumer, in.topic_name, in.part_name, in.offset, sub_name)

	default:
		err = errors.New("the option is not PTP or PSB")
	}

	return err
}

func (s *Server) CheckList() {
	str, _ := os.Getwd()
	str += "/" + name
	ret := CheckFileOrList(str)
	if !ret {
		CreateList(str)
	}
}

const (
	ErrHadStart = "this partition had Start"
)

// 准备接收信息，
// 检查topic和partition是否存在，不存在则需要创建，
// 设置partition中的file和fd，start_index等信息
func (s *Server) PrepareAcceptHandle(in info) (ret string, err error) {
	s.mu.Lock()
	topic, ok := s.topics[in.topic_name]
	if !ok {
		topic = NewTopic(in.topic_name)
		s.topics[in.topic_name] = topic
	}
	s.mu.Unlock()
	return topic.PrepareAcceptHandle(in)
}

// 准备发送信息，
// 检查topic和subscription是否存在，不存在则需要创建
// 检查该文件的config是否存在，不存在则创建，并开启协程
// 协程设置超时时间，时间到则关闭
func (s *Server) PrepareSendHandle(in info) (ret string, err error) {
	//检查或创建topic
	s.mu.Lock()
	topic, ok := s.topics[in.topic_name]
	if !ok {
		topic = NewTopic(in.topic_name)
		s.topics[in.topic_name] = topic
	}
	s.mu.Unlock()
	//检查或创建partition
	return topic.PrepareSendHandle(in)
}

func (s *Server) InfoHandle(ipport string) error {
	DEBUG(dLog, "get consumer's ip_port %v\n", ipport)
	client, err := client_operations.NewClient("clients", client2.WithHostPorts(ipport))
	if err == nil {
		DEBUG(dLog, "connect consumer server successful\n")
		s.mu.Lock()
		consumer, ok := s.consumers[ipport]
		if !ok {
			consumer = NewClient(ipport, client)
			s.consumers[ipport] = consumer
		}
		go s.CheckConsumer(consumer)
		s.mu.Unlock()

		DEBUG(dLog, "return resp to consumer\n")
		return nil
	}
	DEBUG(dError, "Connect client failed\n")
	return err
}

func (s *Server) RecoverConsumer(client *Client) {
	s.mu.Lock()
	client.mu.Lock()
	client.state = ALIVE
	for sub_name, sub := range client.subList {
		go s.topics[sub.topic_name].RecoverRelease(sub_name, client.name)
	}
	client.mu.Unlock()
	s.mu.Unlock()

}

// 检查消费者状态
func (s *Server) CheckConsumer(client *Client) {
	shutdown := client.CheckConsumer()
	if shutdown { //该consumer已关闭，平衡subscription
		client.mu.Lock()
		for _, subscription := range client.subList {
			subscription.ShutdownConsumerInGroup(client.name)

		}
		client.mu.Unlock()
	}
}

// subscribe订阅
func (s *Server) SubHandle(in info) (err error) {
	s.mu.Lock()

	DEBUG(dLog, "get sub information\n")
	top, ok := s.topics[in.topic_name]
	if !ok {
		return errors.New("this topic not in this broker")
	}

	sub, err := top.AddSubScription(in)
	if err != nil {
		s.consumers[in.consumer].AddSubScription(sub)
	}

	s.mu.Unlock()
	return nil
}

func (s *Server) UnSubHandle(in info) error {

	s.mu.Lock()
	top, ok := s.topics[in.topic_name]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub_name, err := top.ReduceSubScription(in)
	if err != nil {
		s.consumers[in.consumer].ReduceSubScription(sub_name)
	}

	s.mu.Unlock()
	return nil
}

func (s *Server) PushHandle(in info) error {
	topic, ok := s.topics[in.topic_name]
	if !ok {
		topic = NewTopic(in.topic_name)
		s.mu.Lock()
		s.topics[in.topic_name] = topic
		s.mu.Unlock()
	}
	topic.addMessage(in)
	return nil
}

func (s *Server) PullHandle(in info) (MSGS, error) {
	/*
		读取index，获得上次的index，写如zookeeper中

	*/

	s.mu.RLock()
	topic, ok := s.topics[in.topic_name]
	s.mu.RUnlock()
	if !ok {
		DEBUG(dError, "this topic is not in this broker\n")
		return MSGS{}, errors.New("this topic is not in this broker\n")
	}
	return topic.PullMessage(in)
}
