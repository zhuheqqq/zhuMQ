package server

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"zhuMQ/client/clients"
	"zhuMQ/kitex_gen/api"
	"zhuMQ/kitex_gen/api/server_operations"
	"zhuMQ/zookeeper"

	"github.com/cloudwego/kitex/client"
)

type ZkServer struct {
	mu              sync.RWMutex
	zk              zookeeper.ZK
	Name            string
	Info_Brokers    map[string]zookeeper.BlockNode
	Info_Topics     map[string]zookeeper.TopicNode
	Info_Partitions map[string]zookeeper.PartitionNode

	Brokers map[string]server_operations.Client //连接各个broker
}

type Info_in struct {
	cli_name   string
	topic_name string
	part_name  string
	blockname  string
	index      int64
	option     int8
	dupnum     int8 //副本数量
}

type Info_out struct {
	Err           error
	broker_name   string
	bro_host_port string
	Ret           string
}

func NewZKServer(zkinfo zookeeper.ZKInfo) *ZkServer {
	return &ZkServer{
		mu: sync.RWMutex{},
		zk: *zookeeper.NewZK(zkinfo),
	}
}

func (z *ZkServer) make(opt Options) {
	z.Name = opt.Name
	z.Info_Brokers = make(map[string]zookeeper.BlockNode)
	z.Info_Topics = make(map[string]zookeeper.TopicNode)
	z.Info_Partitions = make(map[string]zookeeper.PartitionNode)
	z.Brokers = make(map[string]server_operations.Client)
}

// 处理broker信息，如果未连接broker则建立连接
// broker连接到zkserver，zkserver将在zookeeper上监听broker的状态
func (z *ZkServer) HandleBroInfo(bro_name, bro_H_P string) error {
	bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(bro_H_P))
	if err != nil {
		DEBUG(dError, err.Error())
		return err
	}
	z.mu.Lock()
	z.Brokers[bro_name] = bro_cli
	z.mu.Unlock()

	return nil
}

// 获取broker的信息
func (z *ZkServer) ProGetBroker(info Info_in) Info_out {
	//查询zookeeper，获得broker的host_port和name，若未连接则建立连接
	broker, block := z.zk.GetPartNowBrokerNode(info.topic_name, info.part_name)
	PartitionNode := z.zk.GetPartState(info.topic_name, info.part_name)
	//检查该Partition的状态是否设定
	//检查该Partition在Brokers上是否创建raft集群或fetch
	Brokers := make(map[string]string)
	var ret string
	Dups := z.zk.GetDuplicateNodes(block.TopicName, block.PartitionName, block.Name)
	for _, DupNode := range Dups {
		BrokerNode := z.zk.GetBrokerNode(DupNode.BrokerName)
		Brokers[DupNode.BrokerName] = BrokerNode.HostPort
	}

	data, err := json.Marshal(Brokers)
	if err != nil {
		DEBUG(dError, err.Error())
	}

	for BrokerName, BrokerHostPort := range Brokers {
		z.mu.RLock()
		bro_cli, ok := z.Brokers[BrokerName]
		z.mu.RUnlock()

		//未连接该broker
		if !ok {
			bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(BrokerHostPort))
			if err != nil {
				DEBUG(dError, err.Error())
			}
			z.mu.Lock()
			z.Brokers[broker.Name] = bro_cli
			z.mu.Unlock()
		}

		//通知broker检查topic/partition，并创建队列准备接收信息
		resp, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
			TopicName: block.TopicName,
			PartName:  block.PartitionName,
			FileName:  block.FileName,
		})
		if err != nil || !resp.Ret {
			DEBUG(dError, err.Error()+resp.Err)
		}

		//检查该Partition的状态是否设定
		//检查该Partition在Brokers上是否创建raft集群或fetch
		//若该Partition没有设置状态则返回通知producer
		if PartitionNode.Option == -2 { //未设置状态
			ret = "Partition State is -2"
		} else {
			resp, err := bro_cli.PrepareState(context.Background(), &api.PrepareStateRequest{
				TopicName: block.TopicName,
				PartName:  block.PartitionName,
				State:     PartitionNode.Option,
				Brokers:   data,
			})
			if err != nil || !resp.Ret {
				DEBUG(dError, err.Error())
			}
		}
	}

	//返回producer broker的host_port
	return Info_out{
		Err:           err,
		broker_name:   broker.Name,
		bro_host_port: broker.HostPort,
		Ret:           ret,
	}
}

// 创建新的topic节点
func (z *ZkServer) CreateTopic(info Info_in) Info_out {

	//可添加限制数量等操作
	tnode := zookeeper.TopicNode{
		Name: info.topic_name,
	}
	err := z.zk.RegisterNode(tnode)
	return Info_out{
		Err: err,
	}
}

func (z *ZkServer) CreatePart(info Info_in) Info_out {

	//可添加限制数量等操作
	pnode := zookeeper.PartitionNode{
		Name:      info.part_name,
		Index:     int64(1),
		TopicName: info.topic_name,
		Option:    -2,
		PTPoffset: int64(0),
	}

	err := z.zk.RegisterNode(pnode)
	if err != nil {
		return Info_out{
			Err: err,
		}
	}
	//创建NowBlock节点，接收信息
	err = z.CreateNowBlock(info)
	return Info_out{
		Err: err,
	}
}

// 设置Partition的接收信息方式
// 若ack = -1,则为raft同步信息
// 若ack = 1, 则leader写入,	fetch获取信息
// 若ack = 0, 则立即返回,   	fetch获取信息
func (z *ZkServer) SetPartitionState(info Info_in) Info_out {
	var ret string
	node := z.zk.GetPartState(info.topic_name, info.part_name)

	if info.option != node.Option {
		z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
			TopicName: info.topic_name,
			Index:     z.zk.GetPartBlockIndex(info.topic_name, info.part_name),
			Name:      info.part_name,
			Option:    info.option,
			PTPoffset: node.PTPoffset,
			DupNum:    info.dupnum, //需要下面的程序确认，是否能分配一定数量的副本
		})
	}

	//获取该partition
	LeaderBroker, NowBlock := z.zk.GetPartNowBrokerNode(info.topic_name, info.part_name)
	Dups := z.zk.GetDuplicateNodes(NowBlock.TopicName, NowBlock.PartitionName, NowBlock.Name)

	var brokers BrokerS
	for _, DupNode := range Dups {
		BrokerNode := z.zk.GetBrokerNode(DupNode.BrokerName)
		brokers.Brokers[DupNode.BrokerName] = BrokerNode.HostPort
	}

	data_brokers, err := json.Marshal(brokers)
	if err != nil {
		DEBUG(dError, err.Error())
	}

	switch info.option {
	case -1:
		if node.Option == -1 { //与原来的状态相同
			ret = "HadRaft"
		}
		if node.Option == 1 || node.Option == 0 { //原状态为fetch, 关闭原来的写状态, 创建新raft集群
			//查询raft集群的broker, 发送信息
			//fetch操作继续同步之前文件, 创建raft集群,需要更换新文件写入
			//调用CloseAcceptPartition更换文件
			for ice, dupnode := range Dups {
				//停止接收该partition的信息，更换一个新文件写入信息，因为fetch机制一些信息已经写入leader
				//但未写入follower中，更换文件从头写入，重新开启fetch机制为上一个文件同步信息
				lastfilename := z.CloseAcceptPartition(info.topic_name, info.part_name, dupnode.BrokerName, ice)

				bro_cli, ok := z.Brokers[dupnode.BrokerName]
				if !ok {
					// ret := "this partition leader broker is not connected"
					DEBUG(dLog, "this partition(%v) leader broker is not connected\n", info.part_name)
				} else {
					//关闭fetch机制
					resp1, err := bro_cli.CloseFetchPartition(context.Background(), &api.CloseFetchPartitionRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
					})
					if err != nil {
						DEBUG(dError, "%v  err(%v)\n", resp1, err.Error())
					}

					//重新准备接收文件
					resp2, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
						FileName:  "NowBlock.txt",
					})

					if err != nil {
						DEBUG(dError, "%v  err(%v)\n", resp2, err.Error())
					}

					//开启raft集群
					resp3, err := bro_cli.AddRaftPartition(context.Background(), &api.AddRaftPartitionRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
						Brokers:   data_brokers,
					})

					if err != nil {
						DEBUG(dError, "%v  err(%v)\n", resp3, err.Error())
					}

					//开启fetch机制,同步完上一个文件
					resp4, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topic_name,
						PartName:     info.part_name,
						HostPort:     LeaderBroker.HostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     lastfilename,
						Brokers:      data_brokers,
					})

					if err != nil {
						DEBUG(dError, "%v  err(%v)\n", resp4, err.Error())
					}
				}

			}

		}
		if node.Option == -2 { //未创建任何状态, 即该partition未接收过任何信息
			//负载均衡获得一定数量broker节点,并在这些broker上部署raft集群
			for _, dupnode := range Dups {
				bro_cli, ok := z.Brokers[dupnode.BrokerName]
				if !ok {
					DEBUG(dLog, "this partition(%v) leader broker is not connected\n", info.part_name)
				} else {
					//开启raft集群
					resp, err := bro_cli.AddRaftPartition(context.Background(), &api.AddRaftPartitionRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
						Brokers:   data_brokers,
					})

					if err != nil {
						DEBUG(dError, "%v  err(%v)\n", resp, err.Error())
					}
				}
			}
		}

	default:
		if node.Option != -1 { //与原状态相同
			ret = "HadFetch"
		} else { //由raft改为fetch
			//查询fetch的Broker, 发送信息
			//关闭fetch操作, 创建raft集群,不需要更换文件
			for _, dupnode := range Dups {
				//停止接收该partition的信息，当raft被终止后broker server将不会接收该partition的信息
				//NowBlock的文件不需要关闭接收信息，启动fetch机制后，可以继续向该文件写入信息
				// z.CloseAcceptPartition(info.topic_name, info.part_name, dupnode.BrokerName)

				bro_cli, ok := z.Brokers[dupnode.BrokerName]
				if !ok {
					// ret := "this partition leader broker is not connected"
					DEBUG(dLog, "this partition(%v) leader broker is not connected\n", info.part_name)
					// bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(LeaderBroker.HostPort))
					// if err != nil {
					// 	DEBUG(dError, err.Error())
					// }
					// z.mu.Lock()
					// z.Brokers[dupnode.BrokerName] = bro_cli
					// z.mu.Unlock()
				} else {
					//关闭raft集群
					resp1, err := bro_cli.CloseRaftPartition(context.Background(), &api.CloseRaftPartitionRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
					})
					if err != nil {
						DEBUG(dError, "%v  err(%v)\n", resp1, err.Error())
					}

					//开启fetch机制
					resp2, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topic_name,
						PartName:     info.part_name,
						HostPort:     LeaderBroker.HostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     "NowBlock.txt",
						Brokers:      data_brokers,
					})

					if err != nil {
						DEBUG(dError, "%v  err(%v)\n", resp2, err.Error())
					}
				}
			}

		}

		if node.Option == -2 { //未创建任何状态, 即该partition未接收过任何信息
			//负载均衡获得一定数量broker节点,选择一个leader, 并让其他节点fetch leader信息
			for _, dupnode := range Dups {
				bro_cli, ok := z.Brokers[dupnode.BrokerName]
				if !ok {
					DEBUG(dLog, "this partition(%v) leader broker is not connected\n", info.part_name)
				} else {
					//开启fetch机制
					resp3, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topic_name,
						PartName:     info.part_name,
						HostPort:     LeaderBroker.HostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     "NowBlock.txt",
						Brokers:      data_brokers,
					})

					if err != nil {
						DEBUG(dError, "%v  err(%v)\n", resp3, err.Error())
					}
				}
			}
		}
	}
	return Info_out{
		Ret: ret,
	}
}

func (z *ZkServer) CreateNowBlock(info Info_in) error {
	brock_node := zookeeper.BlockNode{
		Name:          "NowBlock",
		FileName:      info.topic_name + info.part_name + "now.txt",
		TopicName:     info.topic_name,
		PartitionName: info.part_name,
		StartOffset:   int64(0),
	}

	return z.zk.RegisterNode(brock_node)
}

func (z *ZkServer) BecomeLeader(info Info_in) error {
	now_block_path := z.zk.TopicRoot + "/" + info.topic_name + "/" + "partitions" + "/" + info.part_name + "/" + "NowBlock"
	NowBlock := z.zk.GetBlockNode(now_block_path)
	NowBlock.LeaderBroker = info.cli_name
	return z.zk.UpdateBlockNode(NowBlock)
}

func (z *ZkServer) SubHandle(info Info_in) error {
	//在zookeeper上创建sub节点，若节点已经存在，则加入group中

	return nil
}

// consumer查询该向那些broker发送请求
// zkserver让broker准备好topic/sub和config
func (z *ZkServer) HandStartGetBroker(info Info_in) (rets []byte, size int, err error) {
	var Parts []zookeeper.Part
	/*
		检查该用户是否订阅了该topic/partition
	*/
	z.zk.CheckSub(zookeeper.StartGetInfo{
		Cli_name:      info.cli_name,
		Topic_name:    info.topic_name,
		PartitionName: info.part_name,
		Option:        info.option,
	})

	//获取该topic或partition的broker,并保证在线,若全部离线则Err
	if info.option == 1 { //ptp_push
		Parts, err = z.zk.GetBrokers(info.topic_name)
	} else if info.option == 3 { //psb_push
		Parts, err = z.zk.GetBroker(info.topic_name, info.part_name, info.index)
	}
	if err != nil {
		return nil, 0, err
	}

	//获取到信息后将通知brokers，让他们检查是否有该Topic/Partition/Subscription/config等
	//并开启Part发送协程，若协程在超时时间到后未收到管道的信息，则关闭该协程
	// var partkeys []clients.PartKey
	// if info.option == 1 || info.option == 3 {
	partkeys := z.SendPreoare(Parts, info)
	// }else{
	// partkeys = GetPartKeys(Parts)
	// }

	data, err := json.Marshal(partkeys)
	if err != nil {
		DEBUG(dError, "turn partkeys to json faile %v", err.Error())
	}

	return data, len(partkeys), nil
}

// push
func (z *ZkServer) SendPreoare(Parts []zookeeper.Part, info Info_in) (partkeys []clients.PartKey) {

	for _, part := range Parts {
		if part.Err != OK {
			partkeys = append(partkeys, clients.PartKey{
				Err: part.Err,
			})
			continue
		}
		z.mu.RLock()
		bro_cli, ok := z.Brokers[part.BrokerName]
		z.mu.RUnlock()

		if !ok {
			bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(part.Host_Port))
			if err != nil {
				DEBUG(dError, "broker(%v) host_port(%v) con't connect %v", part.BrokerName, part.Host_Port, err.Error())
			}
			z.mu.Lock()
			z.Brokers[part.BrokerName] = bro_cli
			z.mu.Unlock()
		}
		rep := &api.PrepareSendRequest{
			TopicName: info.topic_name,
			PartName:  part.Part_name,
			FileName:  part.File_name,
			Option:    info.option,
		}
		if rep.Option == 1 { //ptp
			rep.Offset = part.PTP_index
		} else if rep.Option == 3 { //psb
			rep.Offset = info.index
		}
		resp, err := bro_cli.PrepareSend(context.Background(), rep)
		if err != nil || !resp.Ret {
			DEBUG(dError, "PrepareSend err(%v) error %v", resp.Err, err.Error())
		}

		partkeys = append(partkeys, clients.PartKey{
			Name:        part.Part_name,
			Broker_name: part.BrokerName,
			Broker_H_P:  part.Host_Port,
			Err:         OK,
		})
	}

	return partkeys
}

// 发送请求到broker，关闭该broker上的partition的接收程序
// 并修改NowBlock的文件名，并修改zookeeper上的block信息
func (z *ZkServer) CloseAcceptPartition(topicname, partname, brokername string, ice int) string {

	//获取新文件名
	index := z.zk.GetPartBlockIndex(topicname, partname)
	NewBlockName := "Block_" + string(index)
	NewFileName := NewBlockName + ".txt"

	z.mu.RLock()
	bro_cli, ok := z.Brokers[brokername]
	if !ok {
		DEBUG(dError, "broker(%v) is not connected\n", brokername)
		// bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(LeaderBroker.HostPort))
		// if err != nil {
		// 	DEBUG(dError, err.Error())
		// }
		// z.mu.Lock()
		// z.Brokers[brokername] = bro_cli
		// z.mu.Unlock()
	} else {
		resp, err := bro_cli.CloseAccept(context.Background(), &api.CloseAcceptRequest{
			TopicName:    topicname,
			PartName:     partname,
			Oldfilename:  "NowBlock.txt",
			Newfilename_: NewFileName,
		})
		if err != nil && !resp.Ret {
			DEBUG(dError, err.Error())
		} else {
			str := z.zk.TopicRoot + "/" + topicname + "/Partitions/" + partname + "/" + "NowBlock"
			bnode := z.zk.GetBlockNode(str)

			if ice == 0 {
				//创建新节点
				z.zk.RegisterNode(zookeeper.BlockNode{
					Name:          NewBlockName,
					TopicName:     topicname,
					PartitionName: partname,
					FileName:      NewFileName,
					StartOffset:   resp.Startindex,
					EndOffset:     resp.Endindex,

					LeaderBroker: bnode.LeaderBroker,
				})

				//更新原NowBlock节点信息
				z.zk.UpdateBlockNode(zookeeper.BlockNode{
					Name:          "NowBlock",
					TopicName:     topicname,
					PartitionName: partname,
					FileName:      "NowBlock.txt",
					StartOffset:   resp.Endindex + 1,
					//leader暂时未选出
				})
			}
			//创建该节点下的各个Dup节点
			DupPath := z.zk.TopicRoot + "/" + topicname + "/Partitions/" + partname + "/" + "NowBlock" + "/" + brokername
			DupNode := z.zk.GetDuplicateNode(DupPath)

			DupNode.BlockName = NewBlockName

			z.zk.RegisterNode(DupNode) //在NewBlock上创建副本节点
		}
	}
	z.mu.RUnlock()

	return NewFileName
}

func (z *ZkServer) UpdateOffset(info Info_in) error {
	err := z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
		Name:      info.part_name,
		TopicName: info.topic_name,
		PTPoffset: info.index,
	})
	return err
}

func GetPartKeys(Parts []zookeeper.Part) (partkeys []clients.PartKey) {
	for _, part := range Parts {
		partkeys = append(partkeys, clients.PartKey{
			Name:        part.Part_name,
			Broker_name: part.BrokerName,
			Broker_H_P:  part.Host_Port,
		})
	}
	return partkeys
}

func (z *ZkServer) GetNewLeader(info Info_in) (Info_out, error) {
	block_path := z.zk.TopicRoot + "/" + info.topic_name + "/" + "partitions" + "/" + info.part_name + "/" + info.blockname

	BlockNode := z.zk.GetBlockNode(block_path)
	var LeaderBroker zookeeper.BrokerNode
	//需要检查Leader是否在线，若不在线需要更换leader
	broker_path := z.zk.BrokerRoot + "/" + BlockNode.LeaderBroker
	ret := z.zk.CheckBroker(broker_path)
	if ret {
		LeaderBroker = z.zk.GetBrokerNode(BlockNode.LeaderBroker)
	} else {
		//检查副本中谁的最新，再次检查
		var array []struct {
			EndIndex   int64
			BrokerName string
		}
		Dups := z.zk.GetDuplicateNodes(info.topic_name, info.part_name, info.blockname)
		for _, dup := range Dups {
			str := z.zk.BrokerRoot + "/" + dup.BrokerName
			ret = z.zk.CheckBroker(str)
			if ret {
				//根据EndIndex的大小排序
				array = append(array, struct {
					EndIndex   int64
					BrokerName string
				}{dup.EndOffset, dup.BrokerName})
			}
		}

		sort.SliceStable(array, func(i, j int) bool {
			return array[i].EndIndex > array[j].EndIndex
		})

		for _, arr := range array {
			LeaderBroker = z.zk.GetBrokerNode(arr.BrokerName)
			ret = z.zk.CheckBroker(z.zk.BrokerRoot + "/" + arr.BrokerName)
			if ret {
				break
			}
		}
	}

	return Info_out{
		broker_name:   LeaderBroker.Name,
		bro_host_port: LeaderBroker.HostPort,
	}, nil
}
