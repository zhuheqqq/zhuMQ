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
	Port int    `json:"port"`
	Pnum int    `json:"pnum"`
	//一些负载情况
}

type TopicNode struct {
	Name    string   `json:"name"`
	Pnum    int      `json:"pnum"`
	Brokers []string `json:"brokers"` //保存该topic的partition现在由哪些broker负责
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
	PartitionName string `json:"partitionname"`
	StartOffset   int64  `json:"startoffset"`
	EndOffset     int64  `json:"endoffset"`
	BrokerName    string `json:"brokername"`
}

// 使用反射确定节点类型
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

// 获取Topic的Broker信息
func (z *ZK) GetBrokers(topic string) ([]string, error) {
	path := z.TopicRoot + "/" + topic
	var tnode TopicNode
	ok, _, err := z.conn.Exists(path)
	if !ok || err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	data, _, err := z.conn.Get(path)

	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	err = json.Unmarshal(data, &tnode)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	return tnode.Brokers, nil
}
