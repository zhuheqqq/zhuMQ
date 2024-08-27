package zookeeper

import (
	"encoding/json"
	"github.com/go-zookeeper/zk"
	"reflect"
	"time"
	"zhuMQ/logger"
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
		logger.DEBUG(logger.DError, err.Error())
	}
	return &ZK{
		conn:       conn,
		Root:       info.Root,
		BrokerRoot: info.Root + "/Brokers",
		TopicRoot:  info.Root + "/Topic",
	}
}

type BrokerNode struct {
	Me           int    `json:"me"`
	Name         string `json:"name"`
	BrokHostPort string `json:"brokhostPort"`
	Pnum         int    `json:"pNum"`
	RaftHostPort string `json:"rafthostPort"`
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
	Topic_name    string
	Part_name     string
	BrokerName    string
	BrokHost_Port string
	RaftHost_Port string
	PTP_index     int64
	File_name     string
	Err           string
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
	var snode SubscriptionNode

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
		path += z.TopicRoot + "/" + pnode.TopicName + "/Partitions/" + pnode.Name
		data, err = json.Marshal(pnode)
	case "SubscriptionNode":
		snode = znode.(SubscriptionNode)
		path += z.TopicRoot + "/" + snode.TopicName + "/Subscriptions/" + snode.Name
	case "BlockNode":
		blnode = znode.(BlockNode)
		path += z.TopicRoot + "/" + blnode.TopicName + "/Partitions/" + blnode.PartitionName + "/" + blnode.Name
		data, err = json.Marshal(blnode)
	case "DuplicateNode":
		dnode = znode.(DuplicateNode)
		path += z.TopicRoot + "/" + dnode.TopicName + "/Partitions/" + dnode.PartitionName + "/" + dnode.BlockName + "/" + dnode.Name
		data, err = json.Marshal(dnode)
	}

	if err != nil {
		logger.DEBUG(logger.DError, "the node %v turn json fail%v\n", path, err.Error())
		return err
	}
	ok, _, _ := z.conn.Exists(path)
	if ok {
		logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", path)
		_, sate, _ := z.conn.Get(path)
		z.conn.Set(path, data, sate.Version)
		// return err
	} else {
		_, err = z.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v Creaate fail %v\n", path, err.Error())
			return err
		}
	}

	if i.Name() == "TopicNode" {
		//创建Partitions和Subscriptions
		partitions_path := path + "/" + "Partitions"
		ok, _, err = z.conn.Exists(partitions_path)
		if ok {
			logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", partitions_path)
			return err
		}
		_, err = z.conn.Create(partitions_path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v Creaate fail\n", partitions_path)
			return err
		}

		subscription_apth := path + "/" + "Subscriptions"
		ok, _, err = z.conn.Exists(subscription_apth)
		if ok {
			logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", subscription_apth)
			return err
		}
		_, err = z.conn.Create(subscription_apth, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v Creaate fail\n", subscription_apth)
			return err
		}
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

func (z *ZK) GetPartState(topic_name, part_name string) (PartitionNode, error) {
	var node PartitionNode
	path := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name
	ok, _, err := z.conn.Exists(path)
	logger.DEBUG(logger.DLog, "create broker state %v ok %v\n", path, ok)
	if ok {
		return node, err
	}
	data, _, _ := z.conn.Get(path)

	json.Unmarshal(data, &node)

	return node, nil
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
	path := z.TopicRoot + "/" + topic + "/" + "Partition"
	ok, _, err := z.conn.Exists(path)
	if !ok || err != nil {
		logger.DEBUG(logger.DError, err.Error())
		return nil, err
	}
	var Parts []Part

	partitions, _, _ := z.conn.Children(path)
	for _, part := range partitions {

		PNode, err := z.GetPartitionNode(path + "/" + part)
		if err != nil {
			logger.DEBUG(logger.DError, "get PartitionNode fail%v/%v\n", path, part)
			return nil, err
		}
		PTP_index := PNode.PTPoffset

		var max_dup DuplicateNode
		max_dup.EndOffset = 0
		blocks, _, _ := z.conn.Children(path + "/" + part)
		for _, block := range blocks {
			info, err := z.GetBlockNode(path + "/" + part + "/" + block)
			if err != nil {
				logger.DEBUG(logger.DError, "get block node fail%v/%v/%v\n", part, part, block)
				continue
			}
			if info.StartOffset <= PTP_index && info.EndOffset >= PTP_index {

				Duplicates, _, _ := z.conn.Children(path + "/" + part + "/" + info.Name)
				for _, duplicate := range Duplicates {

					duplicatenode, err := z.GetDuplicateNode(path + "/" + part + "/" + info.Name + "/" + duplicate)
					if err != nil {
						logger.DEBUG(logger.DError, "get dup node fail%v/%v/%v/%v\n", path, part, info.Name, duplicate)
						continue
					}
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
				broker, err := z.GetBrokerNode(max_dup.BrokerName)
				if err != nil {
					logger.DEBUG(logger.DError, "get broker node fail %v\n", max_dup.BlockName)
					continue
				}
				Parts = append(Parts, Part{
					Topic_name:    topic,
					Part_name:     part,
					BrokerName:    broker.Name,
					BrokHost_Port: broker.BrokHostPort,
					RaftHost_Port: broker.RaftHostPort,
					PTP_index:     PTP_index,
					File_name:     info.FileName,
					Err:           ret,
				})
				break
			}
		}
	}

	return Parts, nil
}

func (z *ZK) GetBroker(topic, part string, offset int64) (parts []Part, err error) {
	part_path := z.TopicRoot + "/" + topic + "/Partitions/" + part
	ok, _, err := z.conn.Exists(part_path)
	if !ok || err != nil {
		logger.DEBUG(logger.DError, err.Error())
		return nil, err
	}

	var max_dup DuplicateNode
	max_dup.EndOffset = 0
	blocks, _, _ := z.conn.Children(part_path)
	for _, block := range blocks {
		info, err := z.GetBlockNode(part_path + "/" + block)
		if err != nil {
			logger.DEBUG(logger.DError, "get PartitionNode fail%v/%v\n", part_path, part)
			continue
		}

		if info.StartOffset <= offset && info.EndOffset >= offset {

			Duplicates, _, _ := z.conn.Children(part_path + "/" + block)
			for _, duplicate := range Duplicates {

				duplicatenode, err := z.GetDuplicateNode(part_path + "/" + block + "/" + duplicate)
				if err != nil {
					logger.DEBUG(logger.DError, "get dup node fail %v/%v/%v\n", part_path, block, duplicate)
				}
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
			broker, err := z.GetBrokerNode(max_dup.BrokerName)
			if err != nil {
				logger.DEBUG(logger.DError, "get Broker node fail %v\n", max_dup.BlockName)
				continue
			}
			parts = append(parts, Part{
				Topic_name:    topic,
				Part_name:     part,
				BrokerName:    broker.Name,
				BrokHost_Port: broker.BrokHostPort,
				RaftHost_Port: broker.RaftHostPort,
				File_name:     info.FileName,
				Err:           ret,
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
func (z *ZK) GetPartNowBrokerNode(topic_name, part_name string) (BrokerNode, BlockNode, int8, error) {
	now_block_path := z.TopicRoot + "/" + topic_name + "/" + "Partitions" + "/" + part_name + "/" + "NowBlock"
	for {
		NowBlock, err := z.GetBlockNode(now_block_path)
		if err != nil {
			logger.DEBUG(logger.DError, "get block node fail, path %v err is %v\n", now_block_path, err.Error())
			return BrokerNode{}, BlockNode{}, 0, err
		}
		Broker, err := z.GetBrokerNode(NowBlock.LeaderBroker)
		if err != nil {
			logger.DEBUG(logger.DError, "get broker node fail, Name %v err is %v\n", NowBlock.LeaderBroker, err.Error())
			return BrokerNode{}, NowBlock, 1, err
		}
		logger.DEBUG(logger.DLog, "the Leader Broker is %v\n", NowBlock.LeaderBroker)
		ret := z.CheckBroker(z.BrokerRoot + "/" + Broker.Name)
		if ret {
			return Broker, NowBlock, 2, nil
		} else {
			logger.DEBUG(logger.DLog, "the broker %v is not online\n", Broker.Name)
			time.Sleep(time.Second * 1)
		}
	}
}

func (z *ZK) GetBrokerNode(name string) (BrokerNode, error) {
	path := z.BrokerRoot + "/" + name
	var bronode BrokerNode
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return bronode, err
	}
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &bronode)

	return bronode, nil
}

// 获取Partition节点的PTP索引
func (z *ZK) GetPartitionNode(path string) (PartitionNode, error) {
	var pnode PartitionNode
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return pnode, err
	}
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &pnode)

	return pnode, nil
}

func (z *ZK) GetBlockNode(path string) (BlockNode, error) {
	var blocknode BlockNode
	data, _, err := z.conn.Get(path)
	if err != nil {
		logger.DEBUG(logger.DError, "the block path is %v err is %v\n", path, err.Error())
		return blocknode, err
	}
	json.Unmarshal(data, &blocknode)

	return blocknode, nil
}

func (z *ZK) GetBlockSize(topic_name, part_name string) (int, error) {
	path := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name
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

func (z *ZK) GetDuplicateNode(path string) (DuplicateNode, error) {
	var dupnode DuplicateNode
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return dupnode, err
	}
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &dupnode)

	return dupnode, nil
}

func (z *ZK) GetDuplicateNodes(topic_name, part_name, block_name string) (nodes []DuplicateNode) {
	BlockPath := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name + "/" + block_name
	Dups, _, _ := z.conn.Children(BlockPath)

	for _, dup_name := range Dups {
		DupNode, err := z.GetDuplicateNode(BlockPath + "/" + dup_name)
		if err != nil {
			logger.DEBUG(logger.DError, "the dup %v/%vis not exits\n", BlockPath, dup_name
		} else {
			nodes = append(nodes, DupNode)
		}
	}

	return nodes
}

func (z *ZK) DeleteDupNode(TopicName, PartName, BlockName, DupName string) (ret string, err error) {
	path := z.TopicRoot + "/" + TopicName + "/" + "Partitions" + "/" + PartName + "/" + BlockName + "/" + DupName

	_, sate, _ := z.conn.Get(path)
	err = z.conn.Delete(path, sate.Version)
	if err != nil {
		ret = "delete dupnode fail"
	}

	return ret, err
}

func (z *ZK) UpdateDupNode(dnode DuplicateNode) (ret string, err error) {
	path := z.TopicRoot + "/" + dnode.TopicName + "/" + "Partitions" + "/" + dnode.PartitionName + "/" + dnode.BlockName + "/" + dnode.Name

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

func (z *ZK) GetPartBlockIndex(TopicName, PartName string) (int64, error) {
	str := z.TopicRoot + "/" + TopicName + "/" + "Partitions" + "/" + PartName
	node, err := z.GetPartitionNode(str)
	if err != nil {
		logger.DEBUG(logger.DError, "get partition node fail path is %v err is %v\n", str, err.Error())
		return 0, err
	}

	return node.Index, nil
}
