package server

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
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
	Parts   map[string]*Partition    //储存该主题下的所有分区
	subList map[string]*SubScription //存储该主题所有订阅
}

type Partition struct {
	rmu              sync.RWMutex
	key              string
	queue            []string
	consumer         map[string]*Client
	consumers_offset map[string]int
}

type SubScription struct {
	rmu              sync.RWMutex
	name             string
	topic_name       string
	option           int8 //订阅选项
	consumer_to_part map[string]string
	groups           []*Group
	consistent       *Consistent
}

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

// 创建新的分区对象
func NewPartition(req push) *Partition {
	part := &Partition{
		rmu:              sync.RWMutex{},
		key:              req.key,
		queue:            make([]string, 40),
		consumers_offset: make(map[string]int),
	}

	part.queue = append(part.queue, req.message)

	return part
}

func (p *Partition) AddConsumer(cli *Client) {
	p.rmu.Lock()
	defer p.rmu.Unlock()

	p.consumer[cli.name] = cli
	p.consumers_offset[cli.name] = OFFSET
}

func (p *Partition) DeleteConsumer(cli *Client) {
	p.rmu.Lock()
	defer p.rmu.Unlock()

	delete(p.consumer, cli.name)
	delete(p.consumers_offset, cli.name)
}

// 发布消息给分区中所有消费者
func (p *Partition) Release(server *Server) {
	for consumer_name := range p.consumers_offset {
		server.mu.Lock()
		con := server.consumers[consumer_name]
		server.mu.Unlock()

		go p.Pub(con)
	}
}

// 发布消息给特定的消费者，根据消费者的状态决定是否继续发送消息
func (p *Partition) Pub(cli *Client) {

	for {
		cli.mu.RLock()
		if cli.state == ALIVE {

			name := cli.name
			cli.mu.RUnlock()

			p.rmu.RLock()

			offset := p.consumers_offset[name]
			msg := p.queue[offset]
			p.rmu.RUnlock()

			ret := cli.Pub(msg)
			if ret {
				p.rmu.Lock()
				p.consumers_offset[name] = offset + 1
				p.rmu.Unlock()
			}

		} else {
			cli.mu.RUnlock()
			//time.Sleep(time.Second)
			return
		}
	}
}

func (s *SubScription) Rebalance() {

}

// 创建新topic,初始化分区和订阅对象
func NewTopic(req push) *Topic {
	topic := &Topic{
		rmu:     sync.RWMutex{},
		Parts:   make(map[string]*Partition),
		subList: make(map[string]*SubScription),
	}
	part := NewPartition(req)
	topic.Parts[req.key] = part

	return topic
}

// 根据订阅选项生成订阅字符串
// topic + "nil" + "ptp" (point to point consumer比partition为 1 : n)
// topic + key   + "psb" (pub and sub consumer比partition为 n : 1)
// topic + "nil" + "psb" (pub and sub consumer比partition为 n : n)
func (t *Topic) getStringfromSub(req sub) string {
	ret := req.topic
	if req.option == TOPIC_NIL_PTP { // 订阅发布模式
		ret = ret + "NIL" + "ptp" //point to point
	} else if req.option == TOPIC_KEY_PSB {
		ret = ret + req.key + "psb" //pub and sub
	} else if req.option == TOPIC_NIL_PSB {
		ret = ret + "NIL" + "psb"
	}
	return ret
}

// 添加一个订阅，如果订阅不存在则创建新的订阅，更新分区中的消费者并进行负载均衡
func (t *Topic) AddSubScription(req sub, cli *Client) (retsub *SubScription, err error) {
	ret := t.getStringfromSub(req)

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

	t.Parts.AddConsumer(cli)
	t.Rebalance()

	return subscription, nil
}

// 减少一个订阅，如果订阅存在则删除它，并进行负载均衡
func (t *Topic) ReduceSubScription(req sub) (string, error) {
	ret := t.getStringfromSub(req)
	t.rmu.Lock()
	_, ok := t.subList[ret]
	if !ok {
		return ret, errors.New("This Topic do not have this SubScription")
	}
	delete(t.subList, ret)
	t.rmu.Unlock()

	t.Rebalance()

	return ret, nil
}

// 启动发布，将消息从分区到消费者
func (t *Topic) StartRelease(server *Server) {
	for _, part := range t.Parts {
		part.Release(server) //创建新分片后，开启协程发送消息
	}
}

func (t *Topic) RecoverRelease(sub_name, cli_name string) {

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

	if req.option == TOPIC_NIL_PTP {
		sub.consistent = NewConsistent()
		sub.consistent.Add(req.consumer)
	}

	return sub
}

// 关闭消费者，根据订阅选项处理不同的情况
func (s *SubScription) ShutdownConsumer(cli_name string) string {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch s.option {
	case TOPIC_NIL_PTP: // point to point just one group
		s.groups[0].DownClient(cli_name)
		s.consistent.Reduce(cli_name)
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
		s.consistent.Reduce(cli_name)
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
		s.consistent.Add(req.consumer)
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
		s.consistent.Add(req.consumer)
	case TOPIC_KEY_PSB:
		group := NewGroup(req.topic, req.consumer)
		s.groups = append(s.groups, group)
		s.consumer_to_part[req.consumer] = req.key
	}
}

func Release(sub *SubScription, Part *Partition) {
	switch sub.option {
	case TOPIC_NIL_PTP:
		sub.rmu.RLock()

		Part.rmu.RLock()
		Part.rmu.RUnlock()
		sub.rmu.RUnlock()
	case TOPIC_KEY_PSB:

	}
}

//对TOPIC_NIL_PTP 的情况进行负载均衡，采取一致性哈希的算法
//需要负载均衡的情况

func (t *Topic) Rebalance() {

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
