package server

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"sync"
	"zhuMQ/kitex_gen/api/client_operations"
)

const (
	TOPIC_NIL_PTP = 1 //
	TOPIC_NIL_PSB = 2
	TOPIC_KEY_PSB = 3 //map[cli_name]offset in a partition
	VERTUAL_10    = 10
	VERTUAL_20    = 20

	OFFSET = 0
)

type Topic struct {
	rmu     sync.RWMutex
	Files   map[string]string
	Parts   map[string]*Partition    //储存该主题下的所有分区
	subList map[string]*SubScription //存储该主题所有订阅
}

// 创建新topic,初始化分区和订阅对象
func NewTopic(req push) *Topic {
	topic := &Topic{
		rmu:     sync.RWMutex{},
		Files:   make(map[string]string),
		Parts:   make(map[string]*Partition),
		subList: make(map[string]*SubScription),
	}
	str, _ := os.Getwd()
	str += "/" + name + "/" + req.topic
	CreateList(str)
	part, file := NewPartition(req)
	topic.Files[req.key] = file
	topic.Parts[req.key] = part

	return topic
}

func (t *Topic) GetFile(key string) *File {
	t.rmu.RLock()
	defer t.rmu.RUnlock()

	return t.Parts[key].GetFile()
}

func (t *Topic) addMessage(req push) error {
	part, ok := t.Parts[req.key]
	if !ok {
		part, file := NewPartition(req)
		t.Files[req.key] = file
		t.Parts[req.key] = part
	}

	part.mu.Lock()
	part.addMessage(req)

	part.mu.Unlock()

	return nil
}

// 根据订阅选项生成订阅字符串
// topic + "nil" + "ptp" (point to point consumer比partition为 1 : n)
// topic + key   + "psb" (pub and sub consumer比partition为 n : 1)
// topic + "nil" + "psb" (pub and sub consumer比partition为 n : n)
func GetStringfromSub(top_name, part_name string, option int8) string {
	ret := top_name
	if option == TOPIC_NIL_PTP { // 订阅发布模式
		ret = ret + "NIL" + "ptp" //point to point
	} else if option == TOPIC_KEY_PSB {
		ret = ret + part_name + "psb" //pub and sub
	} else if option == TOPIC_NIL_PSB {
		ret = ret + "NIL" + "psb"
	}
	return ret
}

// 添加一个订阅，如果订阅不存在则创建新的订阅，更新分区中的消费者并进行负载均衡
func (t *Topic) AddSubScription(req sub) (retsub *SubScription, err error) {
	ret := GetStringfromSub(req.topic, req.key, req.option)

	t.rmu.RLock()
	subscription, ok := t.subList[ret]
	t.rmu.RUnlock()

	if !ok {
		subscription = NewSubScription(req, ret)
		t.rmu.Lock()
		t.subList[ret] = subscription
		t.rmu.Unlock()
	} else {
		subscription.AddConsumer(req)
	}

	return subscription, nil
}

// 减少一个订阅，如果订阅存在则删除它，并进行负载均衡
func (t *Topic) ReduceSubScription(req sub) (string, error) {
	ret := GetStringfromSub(req.topic, req.key, req.option)
	t.rmu.Lock()
	subscription, ok := t.subList[ret]
	if !ok {
		return ret, errors.New("This Topic do not have this SubScription")
	} else {
		subscription.ReduceConsumer(req.consumer)
	}
	delete(t.subList, ret)
	t.rmu.Unlock()

	return ret, nil
}

func (t *Topic) RecoverRelease(sub_name, cli_name string) {

}

func (t *Topic) Rebalance() {

}

type Partition struct {
	mu          sync.RWMutex
	file_name   string
	fd          *os.File
	key         string
	file        *File
	queue       []string
	index       int64
	start_index int64
}

// 创建新的分区对象
func NewPartition(req push) (*Partition, string) {
	part := &Partition{
		mu:    sync.RWMutex{},
		key:   req.key,
		queue: make([]string, 50),
	}

	str, _ := os.Getwd()
	str += "/" + name + "/" + req.topic + "/" + req.key + ".txt"
	file, err := CreateFile(str)
	part.file = NewFile(str)
	part.fd = file
	part.file_name = str
	part.index = part.file.GetIndex(file)
	part.start_index = part.index + 1
	if err != nil {
		fmt.Println("create ", str, "failed")
	}
	// part.addMessage(req)

	return part, str
}

// 往队列中添加信息，在队列达到一定长度时将消息写入文件
func (p *Partition) addMessage(req push) {
	p.mu.Lock()
	p.index++
	msg := Message{
		Index:      p.index,
		Topic_name: req.topic,
		Part_name:  req.key,
		Msg:        []byte(req.message),
	}

	p.queue = append(p.queue, msg) // 将新创建的消息对象添加到队列中

	if p.index-p.start_index >= 10 {
		var msg []Message
		for i := 0; i < VERTUAL_10; i++ {
			msg = append(msg, p.queue[i])
		}

		node := Key{
			Start_index: p.start_index,
			End_index:   p.start_index + VERTUAL_10,
		}

		for !p.file.WriteFile(p.fd, node, msg) {
			DEBUG(dError, "write to %v faile\n", p.file_name)
		}
		p.start_index += VERTUAL_10 + 1
		p.queue = p.queue[VERTUAL_10:]
	}
	p.mu.Unlock()
}

func (p *Partition) GetFile() *File {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.file
}

type SubScription struct {
	rmu              sync.RWMutex
	name             string
	topic_name       string
	option           int8 //订阅选项
	consumer_to_part map[string]string
	groups           []*Group
	config           Config
}

func NewSubScription(req sub, name string) *SubScription {
	sub := &SubScription{
		rmu:              sync.RWMutex{},
		name:             name,
		topic_name:       req.topic,
		option:           req.option,
		consumer_to_part: make(map[string]string),
	}

	group := NewGroup(req.topic, req.consumer)
	sub.groups = append(sub.groups, group)
	sub.consumer_to_part[req.consumer] = req.key

	return sub
}

// 关闭消费者，根据订阅选项处理不同的情况
func (s *SubScription) ShutdownConsumer(cli_name string) string {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch s.option {
	case TOPIC_NIL_PTP: // point to point just one group
		s.groups[0].DownClient(cli_name)
	case TOPIC_KEY_PSB:
		for _, group := range s.groups {
			group.DownClient(cli_name)
		}
	}

	return s.topic_name
}

func (s *SubScription) ReduceConsumer(cli_name string) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch s.option {
	case TOPIC_NIL_PTP:
		s.groups[0].DeleteClient(cli_name)
	case TOPIC_KEY_PSB:
		for _, group := range s.groups {
			group.DeleteClient(cli_name)
		}
	}
}

// 恢复消费者，根据订阅选项处理不同的情况
func (s *SubScription) RecoverConsumer(req sub) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch req.option {
	case TOPIC_NIL_PTP:
		s.groups[0].RecoverClient(req.consumer)
	case TOPIC_KEY_PSB:
		group := NewGroup(req.topic, req.consumer)
		s.groups = append(s.groups, group)
		s.consumer_to_part[req.consumer] = req.key
	}
}

// 向订阅添加消费者
func (s *SubScription) AddConsumer(req sub) {

	s.rmu.Lock()
	defer s.rmu.Unlock()
	switch req.option {
	case TOPIC_NIL_PTP:
		s.groups[0].AddClient(req.consumer)
	case TOPIC_KEY_PSB:
		group := NewGroup(req.topic, req.consumer)
		s.groups = append(s.groups, group)
		s.consumer_to_part[req.consumer] = req.key
	}
}

// 提供一个线程安全的配置管理机制
type Config struct {
	mu        sync.RWMutex
	PartToCon map[string][]string
	Clis      map[string]*client_operations.Client

	consistent *Consistent
}

func NewConfig() Config {
	return Config{
		mu:        sync.RWMutex{},
		PartToCon: make(map[string][]string),
		Clis:      make(map[string]*client_operations.Client),
	}
}

func (s *SubScription) GetConfig() *Config {
	s.rmu.RLock()
	defer s.rmu.RUnlock()

	return &(s.config)
}

func (c *Config) AddCli(part_name string, cli_name string, cli *client_operations.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.PartToCon[part_name] = append(c.PartToCon[part_name], cli_name)
	c.Clis[cli_name] = cli
}

func (c *Config) DeleteCli(part_name string, cli_name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.Clis, cli_name)
	for i, name := range c.PartToCon[part_name] {
		if name == cli_name {
			c.PartToCon[part_name] = append(c.PartToCon[part_name][:i], c.PartToCon[part_name][i+1:]...)
			break
		}
	}
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

	mu sync.RWMutex

	//虚拟节点个数
	vertualNodeCount int
}

func NewConsistent() *Consistent {
	con := &Consistent{
		hashSortedNodes:  make([]uint32, 2),
		circle:           make(map[uint32]string),
		nodes:            make(map[string]bool),
		mu:               sync.RWMutex{},
		vertualNodeCount: VERTUAL_10,
	}
	return con
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
	c.nodes[node] = false

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

// 获取键对应的节点，使用一致性哈希计算节点位置
func (c *Consistent) GetNode(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hash := c.hashKey(key)
	i := c.getPosition(hash)

	return c.circle[c.hashSortedNodes[i]]
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
