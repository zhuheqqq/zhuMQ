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
	Name string `json:"name"`
	Host string `json:"host"`
	Pnum int    `json:"pnum"`
	//一些负载情况
}

type TopicNode struct {
	Name string `json:"name"`
	Pnum int    `json:"pnum"`
	//Brokers []string `json:"brokers"` //保存该topic的partition现在由哪些broker负责
	//用于PTP情况
}

type PartitionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topic_name"`
	PTPoffset int64  `json:"ptpoffset"`
}

type BlockNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topic_name"`
	FileName      string `json:"filename"`
	PartitionName string `json:"partitionname"`
	StartOffset   int64  `json:"startoffset"`
	EndOffset     int64  `json:"endoffset"`
	BrokerName    string `json:"brokername"`
}

type Part struct {
	Topic_name string
	Part_name  string
	BrokerName string
	Host_Port  string
	PTP_index  int64
	File_name  string
}

// 使用反射确定节点类型并注册到zookeeper
func (z *ZK) RegisterNode(znode interface{}) (err error) {
	path := ""
	var data []byte
	var bnode BrokerNode
	var tnode TopicNode
	var pnode PartitionNode
	var blnode BlockNode

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
	}

	if err != nil {
		return err
	}

	_, err = z.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
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

	array, _, _ := z.conn.Children(path)
	for _, child := range array {

		PTP_index := z.GetPartitionPTPIndex(path + "/" + child)

		blocks, _, _ := z.conn.Children(path + "/" + child)
		for _, block := range blocks {
			info := z.GetBlockNode(path + "/" + child + "/" + block)

			if info.StartOffset <= PTP_index && info.EndOffset >= PTP_index {
				broker := z.GetBrokerNode(info.TopicName)
				Parts = append(Parts, Part{
					Topic_name: topic,
					Part_name:  child,
					BrokerName: broker.Name,
					Host_Port:  broker.HostPort,
					PTP_index:  PTP_index,
					File_name:  info.FileName,
				})
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
	array, _, _ := z.conn.Children(part_path)
	for _, child := range array {
		info := z.GetBlockNode(part_path + "/" + child)

		if info.StartOffset <= offset && info.EndOffset >= offset {
			broker := z.GetBrokerNode(info.TopicName)
			parts = append(parts, Part{
				Topic_name: topic,
				Part_name:  part,
				BrokerName: info.BrokerName,
				Host_Port:  broker.HostPort,
				File_name:  info.FileName,
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
func (z *ZK) GetPartNowBrokerNode(topic_name, part_name string) (BrokerNode, BlockNode) {
	now_block_path := z.TopicRoot + "/" + topic_name + "/" + "partitions" + "/" + part_name + "/" + "NowBlock"

	NowBlock := z.GetBlockNode(now_block_path)

	Broker := z.GetBrokerNode(NowBlock.BrokerName)

	return Broker, NowBlock
}

// func (z *ZK)

func (z *ZK) GetBrokerNode(name string) BrokerNode {
	path := z.BrokerRoot + "/" + name
	var bronode BrokerNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &bronode)

	return bronode
}

// 获取Partition节点的PTP索引
func (z *ZK) GetPartitionPTPIndex(path string) int64 {
	var pnode PartitionNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &pnode)

	return pnode.PTPoffset
}

func (z *ZK) GetBlockNode(path string) BlockNode {
	var blocknode BlockNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &blocknode)

	return blocknode
}
