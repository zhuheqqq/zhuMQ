package zookeeper

import (
	"encoding/json"
	"fmt"
	"github.com/go-zookeeper/zk"
	"reflect"
	"time"
)

type ZK struct {
	conn *zk.Conn

	Root       string
	BrokerRoot string
	TopicRoot  string
}

type ZKInfo struct {
	HostPorts []string
	Timeout   int
	Root      string
}

// root = "/zhuMQ"
func NewZK(info ZKInfo) *ZK {
	conn, _, err := zk.Connect(info.HostPorts, time.Duration(info.Timeout)*time.Second)
	if err != nil {
		fmt.Println(err.Error())
	}
	return &ZK{
		conn:       conn,
		Root:       info.Root,
		BrokerRoot: info.Root + "/Brokers",
		TopicRoot:  info.Root + "/Topic",
	}
}

type BrokerNode struct {
	Name     string `json:"name"`
	HostPort string `json:"hostPort"`
	Pnum     int    `json:"pNum"`
	//一些负载情况
}

type TopicNode struct {
	Name string `json:"name"`
	Pnum int    `json:"pNum"`
	//Brokers []string `json:"brokers"` //保存该topic的partition现在由哪些broker负责
	//用于PTP情况
}

type PartitionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topic_name"`
	PTPoffset int64  `json:"ptpOffset"`
	Option    int8   `json:"option"`
	DupNum    int8   `json:"dupNum"`
	Index     int64  `json:"index"`
}

type BlockNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topicName"`
	FileName      string `json:"filename"`
	PartitionName string `json:"partitionName"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`
	LeaderBroker  string `json:"brokerBroker"`
}

type DuplicateNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`
	BlockName     string `json:"blockname"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`
	BrokerName    string `json:"brokerName"`
}

type Part struct {
	Topic_name string
	Part_name  string
	BrokerName string
	Host_Port  string
	PTP_index  int64
	File_name  string
	Err        string
}

type SubscriptionNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topic"`
	PartitionName string `json:"part"`
	Option        int8   `json:"option"`
	Groups        []byte `json:"groups"`
}

type Map struct {
	Consumers map[string]bool `json:"consumer"`
}

// 使用反射确定节点类型并注册到zookeeper
func (z *ZK) RegisterNode(znode interface{}) (err error) {
	path := ""
	var data []byte
	var bnode BrokerNode
	var tnode TopicNode
	var pnode PartitionNode
	var blnode BlockNode
	var dnode DuplicateNode

	i := reflect.TypeOf(znode)
	switch i.Name() {
	case "BrokerNode":
		bnode = znode.(BrokerNode)
		path += z.BrokerRoot + "/" + bnode.Name
		data, err = json.Marshal(bnode)
	case "TopicNode":
		tnode = znode.(TopicNode)
		path += z.TopicRoot + "/" + tnode.Name
		data, err = json.Marshal(tnode)
	case "PartitionNode":
		pnode = znode.(PartitionNode)
		path += z.TopicRoot + "/" + pnode.TopicName + "/" + pnode.Name
		data, err = json.Marshal(pnode)
	case "BlockNode":
		blnode = znode.(BlockNode)
		path += z.TopicRoot + "/" + blnode.TopicName + "/" + blnode.PartitionName + "/" + blnode.Name
		data, err = json.Marshal(blnode)
	case "DuplicateNode":
		dnode = znode.(DuplicateNode)
		path += z.TopicRoot + "/" + dnode.TopicName + "/" + dnode.PartitionName + "/" + dnode.BlockName + "/" + dnode.Name
		data, err = json.Marshal(dnode)
	}

	if err != nil {
		return err
	}

	ok, _, err := z.conn.Exists(path)
	if ok {
		return err
	}

	_, err = z.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

func (z *ZK) UpdatePartitionNode(pnode PartitionNode) error {
	path := z.TopicRoot + "/" + pnode.TopicName + "/Partitions/" + pnode.Name
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return err
	}
	data, err := json.Marshal(pnode)
	if err != nil {
		return err
	}
	_, sate, _ := z.conn.Get(path)
	_, err = z.conn.Set(path, data, sate.Version)
	if err != nil {
		return err
	}

	return nil
}

func (z *ZK) UpdateBlockNode(bnode BlockNode) error {
	path := z.TopicRoot + "/" + bnode.TopicName + "/Partitions/" + bnode.PartitionName + "/" + bnode.Name

	ok, _, err := z.conn.Exists(path)
	if !ok {
		return err
	}
	data, err := json.Marshal(bnode)
	if err != nil {
		return err
	}
	_, sate, _ := z.conn.Get(path)
	_, err = z.conn.Set(path, data, sate.Version)
	if err != nil {
		return err
	}

	return nil
}

func (z *ZK) GetPartState(topic_name, part_name string) PartitionNode {
	path := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name
	data, _, _ := z.conn.Get(path)
	var node PartitionNode
	json.Unmarshal(data, &node)

	return node
}

func (z *ZK) CreateState(name string) error {
	path := z.BrokerRoot + "/" + name + "/state"
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return err
	}
	_, err = z.conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

// 检查broker是否在线
func (z *ZK) CheckBroker(broker_path string) bool {
	ok, _, _ := z.conn.Exists(broker_path + "/state")
	if ok {
		return true
	} else {
		return false
	}
}

// 获取某个Topic的所有Broker信息
func (z *ZK) GetBrokers(topic string) ([]Part, error) {
	path := z.TopicRoot + "/" + topic + "/" + "partition"
	ok, _, err := z.conn.Exists(path)
	if !ok || err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	var Parts []Part

	partitions, _, _ := z.conn.Children(path)
	for _, part := range partitions {

		PNode := z.GetPartitionNode(path + "/" + part)
		PTP_index := PNode.PTPoffset

		var max_dup DuplicateNode
		max_dup.EndOffset = 0
		blocks, _, _ := z.conn.Children(path + "/" + part)
		for _, block := range blocks {
			info := z.GetBlockNode(path + "/" + part + "/" + block)
			if info.StartOffset <= PTP_index && info.EndOffset >= PTP_index {

				Duplicates, _, _ := z.conn.Children(path + "/" + part + "/" + info.Name)
				for _, duplicate := range Duplicates {

					duplicatenode := z.GetDuplicateNode(path + "/" + part + "/" + info.Name + "/" + duplicate)
					if max_dup.EndOffset == 0 || max_dup.EndOffset <= duplicatenode.EndOffset {
						//保证broker在线
						if z.CheckBroker(duplicatenode.BrokerName) {
							max_dup = duplicatenode
						}
					}
				}
				var ret string
				if max_dup.EndOffset != 0 {
					ret = "OK"
				} else {
					ret = "thr brokers not online"
				}
				//一个partition只取endoffset最大的broker,其他小的broker副本不全面
				broker := z.GetBrokerNode(max_dup.BrokerName)
				Parts = append(Parts, Part{
					Topic_name: topic,
					Part_name:  part,
					BrokerName: broker.Name,
					Host_Port:  broker.HostPort,
					PTP_index:  PTP_index,
					File_name:  info.FileName,
					Err:        ret,
				})
				break
			}
		}
	}

	return Parts, nil
}

func (z *ZK) GetBroker(topic, part string, offset int64) (parts []Part, err error) {
	part_path := z.TopicRoot + "/" + topic + "/partitions/" + part
	ok, _, err := z.conn.Exists(part_path)
	if !ok || err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	var max_dup DuplicateNode
	max_dup.EndOffset = 0
	blocks, _, _ := z.conn.Children(part_path)
	for _, block := range blocks {
		info := z.GetBlockNode(part_path + "/" + block)

		if info.StartOffset <= offset && info.EndOffset >= offset {

			Duplicates, _, _ := z.conn.Children(part_path + "/" + block)
			for _, duplicate := range Duplicates {

				duplicatenode := z.GetDuplicateNode(part_path + "/" + block + "/" + duplicate)
				if max_dup.EndOffset == 0 || max_dup.EndOffset <= duplicatenode.EndOffset {
					//保证broker在线
					if z.CheckBroker(duplicatenode.BrokerName) {
						max_dup = duplicatenode
					}
				}
			}
			var ret string
			if max_dup.EndOffset != 0 {
				ret = "OK"
			} else {
				ret = "thr brokers not online"
			}
			//一个partition只取endoffset最大的broker,其他小的broker副本不全面
			broker := z.GetBrokerNode(max_dup.BrokerName)
			parts = append(parts, Part{
				Topic_name: topic,
				Part_name:  part,
				BrokerName: broker.Name,
				Host_Port:  broker.HostPort,
				File_name:  info.FileName,
				Err:        ret,
			})
			break
		}
	}
	return parts, nil
}

type StartGetInfo struct {
	Cli_name      string
	Topic_name    string
	PartitionName string
	Option        int8
}

func (z *ZK) CheckSub(info StartGetInfo) bool {

	//检查该consumer是否订阅了该topic或partition

	return true
}

// 获取当前处理某个Topic-Partition的Broker节点信息
// 若Leader不在线，则等待一秒继续请求
func (z *ZK) GetPartNowBrokerNode(topic_name, part_name string) (BrokerNode, BlockNode) {
	now_block_path := z.TopicRoot + "/" + topic_name + "/" + "partitions" + "/" + part_name + "/" + "NowBlock"
	for {
		NowBlock := z.GetBlockNode(now_block_path)
		Broker := z.GetBrokerNode(NowBlock.LeaderBroker)
		ret := z.CheckBroker(z.BrokerRoot + "/" + Broker.Name)
		if ret {
			return Broker, NowBlock
		} else {
			time.Sleep(time.Second * 1)
		}
	}
}

func (z *ZK) GetBrokerNode(name string) BrokerNode {
	path := z.BrokerRoot + "/" + name
	var bronode BrokerNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &bronode)

	return bronode
}

// 获取Partition节点的PTP索引
func (z *ZK) GetPartitionNode(path string) PartitionNode {
	var pnode PartitionNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &pnode)

	return pnode
}

func (z *ZK) GetBlockNode(path string) BlockNode {
	var blocknode BlockNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &blocknode)

	return blocknode
}

func (z *ZK) GetBlockSize(topic_name, part_name string) (int, error) {
	path := z.TopicRoot + "/" + topic_name + "/partitions/" + part_name
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return 0, err
	}

	parts, _, err := z.conn.Children(path)
	if err != nil {
		return 0, err
	}
	return len(parts), nil
}

func (z *ZK) GetDuplicateNode(path string) DuplicateNode {
	var dupnode DuplicateNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &dupnode)

	return dupnode
}

func (z *ZK) GetDuplicateNodes(topic_name, part_name, block_name string) (nodes []DuplicateNode) {
	BlockPath := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name + "/" + block_name
	Dups, _, _ := z.conn.Children(BlockPath)

	for _, dup_name := range Dups {
		DupNode := z.GetDuplicateNode(BlockPath + "/" + dup_name)
		nodes = append(nodes, DupNode)
	}

	return nodes
}

func (z *ZK) DeleteDupNode(TopicName, PartName, BlockName, DupName string) (ret string, err error) {
	path := z.TopicRoot + "/" + TopicName + "/" + "partitions" + "/" + PartName + "/" + BlockName + "/" + DupName

	_, sate, _ := z.conn.Get(path)
	err = z.conn.Delete(path, sate.Version)
	if err != nil {
		ret = "delete dupnode fail"
	}

	return ret, err
}

func (z *ZK) UpdateDupNode(dnode DuplicateNode) (ret string, err error) {
	path := z.TopicRoot + "/" + dnode.TopicName + "/" + "partitions" + "/" + dnode.PartitionName + "/" + dnode.BlockName + "/" + dnode.Name

	data_dnode, err := json.Marshal(dnode)
	if err != nil {
		ret = "DupNode turn byte fail"
		return ret, err
	}
	_, sate, _ := z.conn.Get(path)
	_, err = z.conn.Set(path, data_dnode, sate.Version)
	if err != nil {
		ret = "DupNode Update fail"
	}

	return ret, err
}

func (z *ZK) GetPartBlockIndex(TopicName, PartName string) int64 {
	str := z.TopicRoot + "/" + TopicName + "/" + "partitions" + "/" + PartName
	node := z.GetPartitionNode(str)

	return node.Index
}
