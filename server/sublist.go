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
	Files   map[string]*File
	Parts   map[string]*Partition    //储存该主题下的所有分区
	subList map[string]*SubScription //存储该主题所有订阅
}

// 创建新topic,初始化分区和订阅对象
func NewTopic(req push) *Topic {
	topic := &Topic{
		rmu:     sync.RWMutex{},
		Files:   make(map[string]*File),
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

func (t *Topic) GetParts() map[string]*Partition {
	t.rmu.RLock()
	defer t.rmu.RUnlock()

	return t.Parts
}

func (t *Topic) GetFile(key string) *File {
	t.rmu.RLock()
	defer t.rmu.RUnlock()

	return t.Parts[key].GetFile()
}

func (t *Topic) GetConfig(sub string) *Config {
	t.rmu.RLock()
	defer t.rmu.RUnlock()

	return t.subList[sub].GetConfig()
}

func (t *Topic) addMessage(req push) error {
	part, ok := t.Parts[req.key]
	if !ok {
		part, file := NewPartition(req) //需要向sub中和config中加入一个partition
		t.Files[req.key] = file
		t.Parts[req.key] = part
	}

	part.mu.Lock()

	part.mu.Unlock()

	part.addMessage(req)

	return nil
}

// 根据订阅选项生成订阅字符串
// topic + "nil" + "ptp" (point to point consumer比partition为 1 : n)
// topic + key   + "psb" (pub and sub consumer比partition为 n : 1)按键分发
// topic + "nil" + "psb" (pub and sub consumer比partition为 n : n)广播分发
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
		t.rmu.Lock()
		subscription = NewSubScription(req, ret, t.Parts, t.Files)
		t.subList[ret] = subscription
		t.rmu.Unlock()
	} else {
		subscription.AddConsumerInGroup(req)
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
		subscription.ReduceConsumer(req)
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
	queue       []Message
	index       int64
	start_index int64
}

// 创建新的分区对象
func NewPartition(req push) (*Partition, *File) {
	part := &Partition{
		mu:    sync.RWMutex{},
		key:   req.key,
		queue: make([]Message, 50),
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
	part.addMessage(req)

	return part, part.file
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

	DEBUG(dLog, "part_name %v add message index is %v\n", p.key, p.index)

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

		if !p.file.WriteFile(p.fd, node, msg) {
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
	rmu        sync.RWMutex
	name       string
	topic_name string
	option     int8 //订阅选项
	groups     []*Group
	config     *Config
	partitions map[string]*Partition
	Files      map[string]*File
}

func NewSubScription(req sub, name string, parts map[string]*Partition, files map[string]*File) *SubScription {
	sub := &SubScription{
		rmu:        sync.RWMutex{},
		name:       name,
		topic_name: req.topic,
		option:     req.option,
		partitions: parts,
		Files:      files,
	}

	group := NewGroup(req.topic, req.consumer)
	sub.groups = append(sub.groups, group)
	sub.config = NewConfig(req.topic, len(parts), sub.partitions, sub.Files)

	return sub
}

// 关闭消费者，根据订阅选项处理不同的情况
func (s *SubScription) ShutdownConsumerInGroup(cli_name string) string {
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

func (s *SubScription) ReduceConsumer(req sub) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch req.option {
	case TOPIC_NIL_PTP:
		s.groups[0].DeleteClient(req.consumer)
		s.config.DeleteCli(req.key, req.consumer) //delete config 中的 consumer
	case TOPIC_KEY_PSB:
		group := NewGroup(req.topic, req.consumer)
		s.groups = append(s.groups, group)
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
	}
}

// 向订阅添加消费者
func (s *SubScription) AddConsumerInGroup(req sub) {

	s.rmu.Lock()
	defer s.rmu.Unlock()
	switch req.option {
	case TOPIC_NIL_PTP:
		s.groups[0].AddClient(req.consumer)
	case TOPIC_KEY_PSB:
		group := NewGroup(req.topic, req.consumer)
		s.groups = append(s.groups, group)
	}
}

// 将config中添加consumer   当consumer StartGet是才调用
func (s *SubScription) AddConsumerInConfig(req startget, cli *client_operations.Client) {

	s.rmu.Lock()
	defer s.rmu.Unlock()

	switch req.option {
	case TOPIC_NIL_PTP:

		s.config.AddCli(req.part_name, req.cli_name, cli) //向config中ADD consumer
	case TOPIC_KEY_PSB:
		group := NewGroup(req.topic_name, req.cli_name)
		s.groups = append(s.groups, group)
	}
}

// 提供一个线程安全的配置管理机制，用于管理消息传递系统中的分区和消费者的关系
type Config struct {
	mu sync.RWMutex

	part_num int  //partition数
	cons_num int  //consumer 数
	node_con bool //以消费者为节点还是以分区为节点进行负载均衡

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
		node_con: true,

		PartToCon:  make(map[string][]string),
		Files:      files,
		Partitions: partitions,
		Clis:       make(map[string]*client_operations.Client),
		parts:      make(map[string]*Part),
		consistent: NewConsistent(),
	}

	for partition_name := range partitions {
		con.parts[partition_name] = NewPart(topic_name, partition_name, files[partition_name])
	}

	return con
}

func (s *SubScription) GetConfig() *Config {
	s.rmu.RLock()
	defer s.rmu.RUnlock()

	return s.config
}

// 向Clis加入此consumer的句柄，重新负载均衡，并修改Parts中的clis数组
func (c *Config) AddCli(part_name string, cli_name string, cli *client_operations.Client) {
	c.mu.Lock()

	//consumer > part_num first
	if c.cons_num+1 > c.part_num && c.node_con {
		c.node_con = false

		// node from consumer to partition
		c.consistent = TurnConsistent(GetPartitionArray(c.Partitions))
	}

	c.cons_num++
	c.Clis[cli_name] = cli

	if c.node_con { //consumer is node
		err := c.consistent.Add(cli_name)
		DEBUG(dError, err.Error())
	}
	//else			//partition is node

	c.mu.Unlock()

	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置

}

func (c *Config) DeleteCli(part_name string, cli_name string) {
	c.mu.Lock()

	if c.cons_num-1 <= c.part_num && !c.node_con {
		c.node_con = true

		// node from partition to consumer
		c.consistent = TurnConsistent(GetClisArray(c.Clis))
	}

	c.cons_num--
	delete(c.Clis, cli_name)

	if c.node_con { //consumer is node
		err := c.consistent.Reduce(cli_name)
		DEBUG(dError, err.Error())
	}

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

func (c *Config) AddPartition() {

}

func (c *Config) DeletePartition() {

}

// 负载均衡，将调整后的配置存入PartToCon
func (c *Config) RebalancePtoC() {
	c.mu.Lock()

	parttocon := make(map[string][]string)

	if c.node_con { // node is consumer
		for name := range c.Partitions {
			node := c.consistent.GetNode(name)
			var array []string
			array, ok := parttocon[name]
			array = append(array, node)
			if !ok {
				parttocon[name] = array
			}
		}
	} else { // node is partition
		for name := range c.Clis {
			node := c.consistent.GetNode(name)
			var array []string
			array, ok := parttocon[node]
			array = append(array, name)
			if !ok {
				parttocon[node] = array
			}
		}
	}

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

func GetPartitionArray(partitions map[string]*Partition) []string {
	var array []string

	for key := range partitions {
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
