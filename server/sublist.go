package server

import (
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"sync"
	"zhuMQ/kitex_gen/api/client_operations"
	"zhuMQ/kitex_gen/api/zkserver_operations"
)

const (
	TOPIC_NIL_PTP_PUSH = 1 //PTP---Push
	TOPIC_NIL_PTP_PULL = 2
	TOPIC_KEY_PSB_PUSH = 3 //map[cli_name]offset in a partition
	TOPIC_KEY_PSB_PULL = 4

	TOPIC_NIL_PSB = 10
	VERTUAL_10    = 10
	VERTUAL_20    = 20

	OFFSET = 0
)

type Topic struct {
	rmu     sync.RWMutex
	Files   map[string]*File
	Parts   map[string]*Partition    //储存该主题下的所有分区
	subList map[string]*SubScription //存储该主题所有订阅
	Name    string
}

// 创建新topic,初始化分区和订阅对象
func NewTopic(topic_name string) *Topic {
	topic := &Topic{
		rmu:     sync.RWMutex{},
		Name:    topic_name,
		Files:   make(map[string]*File),
		Parts:   make(map[string]*Partition),
		subList: make(map[string]*SubScription),
	}
	str, _ := os.Getwd()
	str += "/" + Name + "/" + topic_name
	CreateList(str)

	return topic
}

func (t *Topic) PrepareAcceptHandle(in info) (ret string, err error) {
	t.rmu.Lock()
	partition, ok := t.Parts[in.part_name]
	if !ok {
		partition = NewPartition(t.Name, in.part_name)
		t.Parts[in.part_name] = partition
	}

	//设置partition中的file和fd，start_index等信息
	str, _ := os.Getwd()
	str += "/" + Name + "/" + in.topic_name + "/" + in.part_name + "/" + in.file_name

	file, fd, Err, err := NewFile(str)
	if err != nil {
		return Err, err
	}

	t.Files[str] = file
	t.rmu.Unlock()
	ret = partition.StartGetMessage(file, fd, in)
	if ret == OK {
		DEBUG(dLog, "topic(%v)_partition(%v) Start success\n", in.topic_name, in.part_name)
	} else {
		DEBUG(dLog, "topic(%v)_partition(%v) Had Started\n", in.topic_name, in.part_name)
	}
	return ret, nil

}

func (t *Topic) CloseAcceptPart(in info) (start, end int64, ret string, err error) {
	t.rmu.RLock()
	partition, ok := t.Parts[in.part_name]
	t.rmu.RUnlock()
	if !ok {
		ret = "this partition is not in this broker"
		DEBUG(dError, "this partition(%v) is not in this broker\n", in.part_name)
		return 0, 0, ret, errors.New(ret)
	}
	start, end, ret, err = partition.CloseAcceptMessage(in)
	if err != nil {
		DEBUG(dError, err.Error())
	} else {
		str, _ := os.Getwd()
		str += "/" + Name + "/" + in.topic_name + "/" + in.part_name + "/"
		t.rmu.Lock()
		t.Files[str+in.new_name] = t.Files[str+in.file_name]
		delete(t.Files, str+in.file_name)
		t.rmu.Unlock()
	}
	return start, end, ret, err
}

func (p *Partition) CloseAcceptMessage(in info) (start, end int64, ret string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == ALIVE {
		str, _ := os.Getwd()
		str += "/" + Name + "/" + in.topic_name + "/" + in.part_name
		p.file.Update(str, in.new_name) //修改本地文件名
		p.file_name = in.new_name
		p.state = DOWN
		end = p.index
		start = p.file.GetFirstIndex(p.fd)
		p.fd.Close()
	} else if p.state == DOWN {
		ret = "this partition had close"
		DEBUG(dLog, "%v\n", ret)
		err = errors.New(ret)
	}
	return start, end, ret, err
}

func (t *Topic) PrepareSendHandle(in info, zkclient *zkserver_operations.Client) (ret string, err error) {
	sub_name := GetStringfromSub(in.topic_name, in.part_name, in.option)

	t.rmu.Lock()
	//检查或创建partition
	partition, ok := t.Parts[in.part_name]
	if !ok {
		partition = NewPartition(t.Name, in.part_name)
		t.Parts[in.part_name] = partition
	}

	//检查文件是否存在, 若存在为获得File则创建File,若没有则返回错误.
	str, _ := os.Getwd()
	str += "/" + Name + "/" + in.topic_name + "/" + in.part_name + "/" + in.file_name
	file, ok := t.Files[str]
	if !ok {
		file, fd, Err, err := CheckFile(str)
		if err != nil {
			return Err, err
		}
		fd.Close()
		t.Files[str] = file
	}

	//检查或创建sub
	sub, ok := t.subList[sub_name]
	if !ok {
		var sub = NewSubScription(in, sub_name, t.Parts, t.Files)
		t.subList[sub_name] = sub
	}
	//在sub中创建对应文件的config，来等待startget
	t.rmu.Unlock()
	if in.option == 1 {
		ret, err = sub.AddPTPConfig(in, partition, file, zkclient)
	} else if in.option == 3 {
		sub.AddPSBConfig(in, in.part_name, file, zkclient)
	} else if in.option == 2 || in.option == 4 { //PTP_PULL  ||  PSB_PULL
		//在sub中创建一个Node用来保存该consumer的Pull的文件描述符等信息
		sub.AddNode(in, file)
	}

	return ret, err
}

// 处理消费者开始消费某个topic
func (t *Topic) HandleStartToGet(sub_name string, in info, cli *client_operations.Client) (err error) {
	t.rmu.RLock()
	defer t.rmu.RUnlock()
	sub, ok := t.subList[sub_name]
	if !ok {
		ret := "this topic not have this subscription"
		DEBUG(dError, "%v\n", ret)
		return errors.New(ret)
	}
	sub.AddConsumerInConfig(in, cli)
	return nil
}

func (t *Topic) HandleParttitions(Partitions map[string]PartNodeInfo) {
	for part_name := range Partitions {
		_, ok := t.Parts[part_name]
		if !ok {
			part := NewPartition(t.Name, part_name)
			// part.HandleBlocks(topic_name, part_name, partition.Blocks)

			t.Parts[part_name] = part
		} else {
			DEBUG(dWarn, "This topic(%v) part(%v) had in s.topics\n", t.Name, part_name)
		}
	}
}

func (t *Topic) GetParts() map[string]*Partition {
	t.rmu.RLock()
	defer t.rmu.RUnlock()

	return t.Parts
}

func (t *Topic) GetFile(in info) *File {
	t.rmu.Lock()
	str, _ := os.Getwd()
	str += "/" + Name + "/" + in.topic_name + "/" + in.part_name + "/" + in.file_name
	File, ok := t.Files[str]
	if !ok {
		file, fd, Err, err := NewFile(str)
		if err != nil {
			DEBUG(dError, "Err(%v), err(%v)", Err, err.Error())
			return nil
		}
		fd.Close()
		t.Files[str] = file
	}
	t.rmu.Unlock()
	return File
}

func (t *Topic) GetConfig(sub string) *Config {
	t.rmu.RLock()
	defer t.rmu.RUnlock()

	return t.subList[sub].GetConfig()
}

func (t *Topic) AddPartition(part_name string) {
	part := NewPartition(t.Name, part_name)
	t.Parts[part_name] = part
}

func (t *Topic) addMessage(in info) error {
	part, ok := t.Parts[in.part_name]
	if !ok {
		DEBUG(dError, "not find this part in add message\n")
		part := NewPartition(in.topic_name, in.part_name) //需要向sub中和config中加入一个partition
		t.Parts[in.part_name] = part
	}

	part.mu.Lock()

	part.mu.Unlock()

	part.AddMessage(in)

	return nil
}

func (t *Topic) PullMessage(in info) (MSGS, error) {
	sub_name := GetStringfromSub(in.topic_name, in.part_name, in.option)
	t.rmu.RLock()
	sub, ok := t.subList[sub_name]
	t.rmu.RUnlock()
	if !ok {
		DEBUG(dError, "this topic is not have sub(%v)\n", sub_name)
		return MSGS{}, errors.New("this topic is not have this sub")
	}

	return sub.PullMsgs(in)
}

const (
	START = "start"
	CLOSE = "close"
)

// 根据订阅选项生成订阅字符串
// topic + "nil" + "ptp" (point to point consumer比partition为 1 : n)
// topic + key   + "psb" (pub and sub consumer比partition为 n : 1)按键分发
// topic + "nil" + "psb" (pub and sub consumer比partition为 n : n)广播分发
func GetStringfromSub(top_name, part_name string, option int8) string {
	ret := top_name
	if option == TOPIC_NIL_PTP_PUSH || option == TOPIC_NIL_PTP_PULL {
		ret = ret + "NIL" + "ptp" //point to point
	} else if option == TOPIC_KEY_PSB_PUSH || option == TOPIC_KEY_PSB_PULL {
		ret = ret + part_name + "psb" //pub and sub
		//} else if option == TOPIC_NIL_PSB {
		//	ret = ret + "NIL" + "psb"
	}
	return ret
}

// 添加一个订阅，如果订阅不存在则创建新的订阅，更新分区中的消费者并进行负载均衡
func (t *Topic) AddSubScription(in info) (retsub *SubScription, err error) {
	ret := GetStringfromSub(in.topic_name, in.part_name, in.option)
	t.rmu.RLock()
	subscription, ok := t.subList[ret]
	t.rmu.RUnlock()

	if !ok {
		t.rmu.Lock()
		subscription = NewSubScription(in, ret, t.Parts, t.Files)
		t.subList[ret] = subscription
		t.rmu.Unlock()
	} else {
		subscription.AddConsumerInGroup(in)
	}

	return subscription, nil
}

// 减少一个订阅，如果订阅存在则删除它，并进行负载均衡
func (t *Topic) ReduceSubScription(in info) (string, error) {
	ret := GetStringfromSub(in.topic_name, in.part_name, in.option)
	t.rmu.Lock()
	subscription, ok := t.subList[ret]
	if !ok {
		return ret, errors.New("This Topic do not have this SubScription")
	} else {
		subscription.ReduceConsumer(in)
	}
	delete(t.subList, ret)
	t.rmu.Unlock()

	return ret, nil
}

func (t *Topic) RecoverRelease(sub_name, cli_name string) {

}

type Partition struct {
	mu          sync.RWMutex
	file_name   string
	fd          *os.File
	key         string
	file        *File
	queue       []Message
	index       int64
	start_index int64
	state       string
}

// 创建新的分区对象
func NewPartition(topic_name, part_name string) *Partition {
	part := &Partition{
		mu:    sync.RWMutex{},
		key:   part_name,
		state: CLOSE,
		//queue: make([]Message, 50),
	}

	str, _ := os.Getwd()
	str += "/" + Name + "/" + topic_name + "/" + part_name

	return part
}

func (p *Partition) StartGetMessage(file *File, fd *os.File, in info) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	ret := ""
	switch p.state {
	case ALIVE:
		ret = ErrHadStart
	case CLOSE:
		//p.queue = make([]Message, 50)

		p.state = ALIVE
		p.file = file
		p.fd = fd
		p.file_name = in.file_name
		p.index = file.GetIndex(fd)
		p.start_index = p.index + 1
		ret = OK
	}
	return ret
}

// 检查是否存在path的文件，若不存在则错误，存在则创建一个File
// 若path和partition的name相同，有就创建一个File，没有就创建一个这个名字的文件
//func (p *Partition) AddFile(path string) *File {
//	return
//}

// 检查state
// 当接收数据达到一定数量将修改zookeeper上的index
func (p *Partition) AddMessage(in info) (ret string, err error) {
	p.mu.Lock()
	if p.state == DOWN {
		ret = "this partition close accept"
		DEBUG(dLog, "%v\n", ret)
		return ret, errors.New(ret)
	}
	p.index++
	msg := Message{
		Index:      p.index,
		Size:       in.size,
		Topic_name: in.topic_name,
		Part_name:  in.part_name,
		Msg:        in.message,
	}

	//DEBUG(dLog, "part_name %v add message index is %v\n", p.key, p.index)

	p.queue = append(p.queue, msg) // 将新创建的消息对象添加到队列中

	//达到一定大小后写入磁盘
	if p.index-p.start_index >= VERTUAL_10 {
		var msg []Message
		for i := 0; i < VERTUAL_10; i++ {
			msg = append(msg, p.queue[i])
		}

		node := Key{
			Start_index: p.start_index,
			End_index:   p.start_index + VERTUAL_10 - 1,
		}

		data_msg, err := json.Marshal(msg)
		if err != nil {
			DEBUG(dError, "%v turn json fail\n", msg)
		}
		node.Size = len(data_msg)

		if !p.file.WriteFile(p.fd, node, data_msg) {
			DEBUG(dError, "write to %v faile\n", p.file_name)
		}
		p.start_index += VERTUAL_10 + 1
		p.queue = p.queue[VERTUAL_10:]
	}
	p.mu.Unlock()

	return ret, err
}

func (p *Partition) GetFile() *File {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.file
}

type SubScription struct {
	rmu        sync.RWMutex
	name       string
	topic_name string
	option     int8 //订阅选项
	groups     []*Group

	//需要修改，分为多种订阅，每种订阅方式一种config
	PTP_config *Config
	//partition_name + consumer_name  to config
	PSB_configs map[string]*PSBConfig_PUSH

	//一个consumer向文件描述符等的映射,每次pull将使用上次的文件描述符等资源
	//topic+partition+consumer   to  Node
	nodes      map[string]*Node
	partitions map[string]*Partition
	Files      map[string]*File
}

func NewSubScription(in info, name string, parts map[string]*Partition, files map[string]*File) *SubScription {
	sub := &SubScription{
		rmu:         sync.RWMutex{},
		name:        name,
		topic_name:  in.topic_name,
		option:      in.option,
		partitions:  parts,
		Files:       files,
		PTP_config:  nil,
		PSB_configs: make(map[string]*PSBConfig_PUSH),
	}

	group := NewGroup(in.part_name, in.consumer)
	sub.groups = append(sub.groups, group)

	return sub
}

// 当有消费者需要开始消费时，PTP
// 若sub中该文件的config存在，则加入该config
// 若sub中该文件的config不存在，则创建一个config，并加入
func (s *SubScription) AddPTPConfig(in info, partition *Partition, file *File, zkclient *zkserver_operations.Client) (ret string, err error) {
	s.rmu.RLock()
	if s.PTP_config == nil {
		s.PTP_config = NewConfig(in.topic_name, 0, nil, nil)
	}

	err = s.PTP_config.AddPartition(in, partition, file, zkclient)
	s.rmu.RUnlock()
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func (s *SubScription) AddPSBConfig(in info, part_name string, file *File, zkclient *zkserver_operations.Client) {

	s.rmu.Lock()
	_, ok := s.PSB_configs[part_name+in.consumer]
	if !ok {
		config := NewPSBConfigPush(in, file, zkclient)
		s.PSB_configs[part_name+in.consumer] = config
	} else {
		DEBUG(dLog, "This PSB has Start\n")
	}

	s.rmu.Unlock()
}

func (s *SubScription) AddNode(in info, file *File) {
	str := in.topic_name + in.part_name + in.consumer
	s.rmu.Lock()
	_, ok := s.nodes[str]
	if !ok {
		node := NewNode(in, file)
		s.nodes[str] = node
	}
	s.rmu.Unlock()
}

// 关闭消费者，根据订阅选项处理不同的情况
func (s *SubScription) ShutdownConsumerInGroup(cli_name string) string {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch s.option {
	case TOPIC_NIL_PTP_PUSH: // point to point just one group
		s.groups[0].DownClient(cli_name)
	case TOPIC_KEY_PSB_PUSH:
		for _, group := range s.groups {
			group.DownClient(cli_name)
		}
	}

	return s.topic_name
}

func (s *SubScription) ReduceConsumer(in info) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.groups[0].DeleteClient(in.consumer)
		s.PTP_config.DeleteCli(in.part_name, in.consumer) //delete config 中的 consumer
	case TOPIC_KEY_PSB_PUSH:
		for _, group := range s.groups {
			group.DeleteClient(in.consumer)
		}
	}
}

// 恢复消费者，根据订阅选项处理不同的情况
func (s *SubScription) RecoverConsumer(in info) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.groups[0].RecoverClient(in.consumer)
	case TOPIC_KEY_PSB_PUSH:
		group := NewGroup(in.topic_name, in.consumer)
		s.groups = append(s.groups, group)
	}
}

// 向订阅添加消费者
func (s *SubScription) AddConsumerInGroup(in info) {

	s.rmu.Lock()
	defer s.rmu.Unlock()
	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.groups[0].AddClient(in.consumer)
	case TOPIC_KEY_PSB_PUSH:
		group := NewGroup(in.topic_name, in.consumer)
		s.groups = append(s.groups, group)
	}
}

// 将config中添加consumer   当consumer StartGet是才调用
func (s *SubScription) AddConsumerInConfig(in info, cli *client_operations.Client) {

	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch in.option {
	case TOPIC_NIL_PTP_PUSH:

		s.PTP_config.AddCli(in.consumer, cli) //向config中ADD consumer
	case TOPIC_KEY_PSB_PUSH:
		config, ok := s.PSB_configs[in.part_name+in.consumer]
		if !ok {
			DEBUG(dError, "this PSBconfig PUSH id not been\n")
		}
		config.Start(in, cli)
	}
}

func (s *SubScription) PullMsgs(in info) (MSGS, error) {
	node_name := in.topic_name + in.part_name + in.consumer
	s.rmu.RLock()
	node, ok := s.nodes[node_name]
	s.rmu.RUnlock()
	if !ok {
		DEBUG(dError, "this sub has not have this node(%v)\n", node_name)
		return MSGS{}, errors.New("this sub has not have this node")
	}
	return node.ReadMSGS(in)
}

// 提供一个线程安全的配置管理机制，用于管理消息传递系统中的分区和消费者的关系
type Config struct {
	mu sync.RWMutex

	part_num int //partition数
	cons_num int //consumer 数

	part_close chan *Part

	PartToCon map[string][]string

	Partitions map[string]*Partition
	Files      map[string]*File
	Clis       map[string]*client_operations.Client

	parts map[string]*Part //PTP的Part   partition_name to Part

	consistent *Consistent //consumer 	<= partition

}

func NewConfig(topic_name string, part_num int, partitions map[string]*Partition, files map[string]*File) *Config {
	con := &Config{
		mu:       sync.RWMutex{},
		part_num: part_num,
		cons_num: 0,

		part_close: make(chan *Part),
		PartToCon:  make(map[string][]string),
		Files:      files,
		Partitions: partitions,
		Clis:       make(map[string]*client_operations.Client),
		parts:      make(map[string]*Part),
		consistent: NewConsistent(),
	}

	go con.GetCloseChan(con.part_close)

	return con
}

func (c *Config) GetCloseChan(ch chan *Part) {
	for close := range c.part_close {
		c.DeletePartition(close.part_name, close.file)
	}
}

func (s *SubScription) GetConfig() *Config {
	s.rmu.RLock()
	defer s.rmu.RUnlock()

	return s.PTP_config
}

// 向Clis加入此consumer的句柄，重新负载均衡，并修改Parts中的clis数组
func (c *Config) AddCli(cli_name string, cli *client_operations.Client) {
	c.mu.Lock()

	c.cons_num++
	c.Clis[cli_name] = cli

	err := c.consistent.Add(cli_name)
	if err != nil {
		DEBUG(dError, err.Error())
	}

	c.mu.Unlock()

	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置

}

func (c *Config) DeleteCli(part_name string, cli_name string) {
	c.mu.Lock()

	c.cons_num--
	delete(c.Clis, cli_name)

	err := c.consistent.Reduce(cli_name)
	DEBUG(dError, err.Error())

	c.mu.Unlock()

	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置

	for i, name := range c.PartToCon[part_name] {
		if name == cli_name {
			c.PartToCon[part_name] = append(c.PartToCon[part_name][:i], c.PartToCon[part_name][i+1:]...)
			break
		}
	}
}

func (c *Config) AddPartition(in info, partition *Partition, file *File, zkclient *zkserver_operations.Client) error {
	c.mu.Lock()

	c.part_num++
	c.Partitions[in.part_name] = partition
	c.Files[file.filename] = file

	c.parts[in.part_name] = NewPart(in, file, zkclient)
	c.parts[in.part_name].Start(c.part_close)
	c.mu.Unlock()

	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
	return nil
}

// part消费完成，移除config中的Partition和Part
func (c *Config) DeletePartition(part_name string, file *File) {
	c.mu.Lock()

	c.part_num--
	delete(c.Partitions, part_name)
	delete(c.Files, file.filename)

	//该Part协程已经关闭，该partition的文件已经消费完毕，
	c.mu.Unlock()

	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
}

// 负载均衡，将调整后的配置存入PartToCon
// 将Consisitent中的ConH置false, 循环两次Partitions
// 第一次拿取 1个 Consumer
// 第二次拿取 靠前的 ConH 为 true 的 Consumer
// 直到遇到ConH为 false 的
func (c *Config) RebalancePtoC() {

	c.consistent.SetFreeNode() //将空闲节点设为len(consumers)
	c.consistent.TurnZero()    //将conusmer全设为空闲

	parttocon := make(map[string][]string)

	c.mu.RLock()
	Parts := c.Partitions
	c.mu.RUnlock()

	for name := range Parts {
		node := c.consistent.GetNode(name)
		var array []string
		array, ok := parttocon[name]
		array = append(array, node)
		if !ok {
			parttocon[name] = array
		}
	}

	for {
		for name := range Parts {
			if c.consistent.GetFreeNodeNum() > 0 {
				node := c.consistent.GetNodeFree(name)
				var array []string
				array, ok := parttocon[name]
				array = append(array, node)
				if !ok {
					parttocon[name] = array
				}
			} else {
				break
			}
		}
		if c.consistent.GetFreeNodeNum() <= 0 {
			break
		}
	}
	c.mu.Lock()
	c.PartToCon = parttocon
	c.mu.Unlock()
}

// 根据PartToCon中的配置，更新Parts中的Clis
func (c *Config) UpdateParts() {

	c.mu.RLock()
	for partition_name, part := range c.parts {
		part.UpdateClis(c.PartToCon[partition_name], c.Clis)
	}
	c.mu.RUnlock()
}

//对TOPIC_NIL_PTP 的情况进行负载均衡，采取一致性哈希的算法
//需要负载均衡的情况

// 用于负载均衡
type Consistent struct {
	//排序的hash虚拟节点（环形）
	hashSortedNodes []uint32

	//虚拟节点（consumer）对应的实际节点
	circle map[uint32]string

	//已绑定的consumer为true
	nodes map[string]bool

	// cconsumer以负责一个Partition则为true
	ConH     map[string]bool
	FreeNode int

	mu sync.RWMutex

	//虚拟节点个数
	vertualNodeCount int
}

func NewConsistent() *Consistent {
	con := &Consistent{
		hashSortedNodes:  make([]uint32, 2),
		circle:           make(map[uint32]string),
		nodes:            make(map[string]bool),
		ConH:             make(map[string]bool),
		FreeNode:         0,
		mu:               sync.RWMutex{},
		vertualNodeCount: VERTUAL_10,
	}
	return con
}

func GetPartitionArray(partitions map[string]*Partition) []string {
	var array []string

	for key := range partitions {
		array = append(array, key)
	}

	return array
}

func GetConsumerArray(consumers map[string]*client_operations.Client) []string {
	var array []string

	for key := range consumers {
		array = append(array, key)
	}

	return array
}

func TurnConsistent(nodes []string) *Consistent {
	newconsistent := NewConsistent()

	for _, node := range nodes {
		newconsistent.Add(node)
	}

	return newconsistent
}

func (c *Consistent) SetFreeNode() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.FreeNode = len(c.ConH)
}

func (c *Consistent) GetFreeNodeNum() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.FreeNode
}

// 计算键的哈希值
func (c *Consistent) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// add consumer name as node
func (c *Consistent) Add(node string) error {
	if node == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.nodes[node]; ok {
		return errors.New("node already existed")
	}
	c.nodes[node] = true
	c.ConH[node] = false

	for i := 0; i < c.vertualNodeCount; i++ {
		virtualKey := c.hashKey(node + strconv.Itoa(i))
		c.circle[virtualKey] = node
		c.hashSortedNodes = append(c.hashSortedNodes, virtualKey)
	}

	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})
	return nil
}

func (c *Consistent) Reduce(node string) error {
	if node == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.nodes[node]; !ok {
		return errors.New("node already delete")
	}
	delete(c.nodes, node)
	delete(c.ConH, node)

	for i := 0; i < c.vertualNodeCount; i++ {
		virtualKey := c.hashKey(node + strconv.Itoa(i))
		delete(c.circle, virtualKey)
		for j := 0; j < len(c.hashSortedNodes); j++ {
			if c.hashSortedNodes[j] == virtualKey && j != len(c.hashSortedNodes)-1 {
				c.hashSortedNodes = append(c.hashSortedNodes[:j], c.hashSortedNodes[j+1:]...)
			} else if j == len(c.hashSortedNodes)-1 {
				c.hashSortedNodes = c.hashSortedNodes[:j]
			}
		}
	}

	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})
	return nil
}

func (c *Consistent) TurnZero() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.ConH {
		c.ConH[key] = false
	}
}

// 获取键对应的节点，使用一致性哈希计算节点位置
func (c *Consistent) GetNode(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := c.hashKey(key)
	i := c.getPosition(hash)

	con := c.circle[c.hashSortedNodes[i]]

	c.ConH[con] = true
	c.FreeNode--

	return con
}

func (c *Consistent) GetNodeFree(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := c.hashKey(key)
	i := c.getPosition(hash)

	i += 1
	for {
		if i == len(c.hashSortedNodes)-1 {
			i = 0
		}
		con := c.circle[c.hashSortedNodes[i]]
		// fmt.Println("Free Node nums is ", c.FreeNode)
		if !c.ConH[con] {
			c.ConH[con] = true
			c.FreeNode--
			return con
		}
		i++
	}
}

// 获取哈希值在排序后的哈希节点列表中的位置
func (c *Consistent) getPosition(hash uint32) int {
	i := sort.Search(len(c.hashSortedNodes), func(i int) bool { return c.hashSortedNodes[i] >= hash })

	if i == len(c.hashSortedNodes) {
		if i == len(c.hashSortedNodes)-1 {
			return 0
		} else {
			return 1
		}
	} else {
		return len(c.hashSortedNodes) - 1
	}
}

type PSBConfig_PUSH struct {
	mu sync.RWMutex

	part_close chan *Part
	file       *File

	Cli  *client_operations.Client
	part *Part //PTP的Part   partition_name to Part
}

func NewPSBConfigPush(in info, file *File, zkclient *zkserver_operations.Client) *PSBConfig_PUSH {
	ret := &PSBConfig_PUSH{
		mu:         sync.RWMutex{},
		part_close: make(chan *Part),
		file:       file,
		part:       NewPart(in, file, zkclient),
	}

	// ret.part.Start(ret.part_close)

	return ret
}

func (pc *PSBConfig_PUSH) Start(in info, cli *client_operations.Client) {
	pc.mu.Lock()
	pc.Cli = cli
	var names []string
	clis := make(map[string]*client_operations.Client)
	names = append(names, in.consumer)
	clis[in.consumer] = cli
	pc.part.UpdateClis(names, clis)
	pc.part.Start(pc.part_close)
	pc.mu.Unlock()
}
