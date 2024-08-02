package server

import (
	"context"
	"fmt"
	"github.com/cloudwego/kitex/server"
	"zhuMQ/kitex_gen/api"
	"zhuMQ/kitex_gen/api/server_operations"
	"zhuMQ/kitex_gen/api/zkserver_operations"
	"zhuMQ/zookeeper"
)

// RPCServer可以注册两种server，
// 一个是客户端（生产者和消费者连接的模式，负责订阅等请求和对zookeeper的请求）使用的server
// 另一个是Broker获取Zookeeper信息和调度各个Broker的调度者
type RPCServer struct {
	name     string
	srv_cli  server.Server
	srv_bro  server.Server
	zkinfo   zookeeper.ZKInfo
	server   *Server
	zkserver *ZkServer
}

func NewRpcServer(zkinfo zookeeper.ZKInfo) RPCServer {
	LOGinit()
	return RPCServer{
		zkinfo: zkinfo,
	}
}

func (s *RPCServer) Start(opts_cli, opts_bro []server.Option, opt Options) error {

	switch opt.Tag {
	case BROKER:
		s.server = NewServer(s.zkinfo)
		s.server.make(opt)
	case ZKBROKER:
		s.zkserver = NewZKServer(s.zkinfo)
		s.zkserver.make(opt)

		srv_bro := zkserver_operations.NewServer(s, opts_bro...)
		s.srv_bro = srv_bro
		DEBUG(dLog, "Broker start rpcserver for brokers\n")
		go func() {
			err := srv_bro.Run()

			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	srv_cli := server_operations.NewServer(s, opts_cli...)
	s.srv_cli = srv_cli
	DEBUG(dLog, "Broker start rpcserver for clients\n")
	go func() {
		err := srv_cli.Run()
		if err != nil {
			fmt.Println(err.Error())
		}
	}()

	return nil
}

// producer--->broker server
func (s *RPCServer) ShutDown_server() {
	s.srv_cli.Stop()
	s.srv_bro.Stop()
}

// 处理推送请求
func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (resp *api.PushResponse, err error) {
	fmt.Println(req)
	err = s.server.PushHandle(info{
		producer:   req.Producer,
		topic_name: req.Topic,
		part_name:  req.Key,
		message:    req.Message,
	})
	if err == nil {
		return &api.PushResponse{Ret: false}, nil
	}
	return &api.PushResponse{Ret: false}, err
}

// 处理拉取请求
func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (resp *api.PullResponse, err error) {
	ret, err := s.server.PullHandle(info{
		consumer:   req.Consumer,
		topic_name: req.Topic,
		part_name:  req.Key,
	})
	if err == nil {
		return &api.PullResponse{Message: ret.message}, nil
	}
	return &api.PullResponse{
		Message: "error", //有错误默认返回error
	}, nil
}

// consumer---->broker server
func (s *RPCServer) ConInfo(ctx context.Context, req *api.InfoRequest) (resp *api.InfoResponse, err error) {
	//get client_server's ip and port

	err = s.server.InfoHandle(req.IpPort)
	if err == nil {
		return &api.InfoResponse{Ret: true}, nil
	}

	return &api.InfoResponse{Ret: false}, err
}

// consumer---->broker server
func (s *RPCServer) StarttoGet(ctx context.Context, req *api.InfoGetRequest) (resp *api.InfoGetResponse, err error) {
	err = s.server.StartGet(info{
		consumer:   req.CliName,
		topic_name: req.TopicName,
		part_name:  req.PartName,
		offset:     req.Offset,
	})
	if err == nil {
		return &api.InfoGetResponse{
			Ret: true,
		}, nil
	}
	return &api.InfoGetResponse{Ret: false}, err
}

// 先在zookeeper上创建一个Topic，当生产该信息时，或消费信息时再有zkserver发送信息到broker
// 让broker创建
func (s *RPCServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (r *api.CreateTopicResponse, err error) {
	info := s.zkserver.CreateTopic(Info_in{
		topic_name: req.TopicName,
	})

	if info.Err != nil {
		return &api.CreateTopicResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.CreateTopicResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// 先在zookeeper上创建一个Partition，当生产该信息时，或消费信息时再有zkserver发送信息到broker
// 让broker创建
func (s *RPCServer) CreatePart(ctx context.Context, req *api.CreatePartRequest) (r *api.CreatePartResponse, err error) {
	info := s.zkserver.CreateTopic(Info_in{
		topic_name: req.TopicName,
		part_name:  req.PartName,
	})

	if info.Err != nil {
		return &api.CreatePartResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.CreatePartResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// producer 获取该向那个broker发送信息
func (s *RPCServer) ProGetBroker(ctx context.Context, req *api.ProGetBrokRequest) (r *api.ProGetBrokResponse, err error) {
	info := s.zkserver.ProGetBroker(Info_in{
		topic_name: req.TopicName,
		part_name:  req.PartName,
	})

	if info.Err != nil {
		return &api.ProGetBrokResponse{
			Ret: false,
		}, info.Err
	}

	return &api.ProGetBrokResponse{
		Ret:            true,
		BrokerHostPort: info.bro_host_port,
	}, nil
}

// consumer---->zkserver
func (s *RPCServer) ConStartGetBroker(ctx context.Context, req *api.ConStartGetBrokRequest) (r *api.ConStartGetBrokResponse, err error) {
	parts, size, err := s.zkserver.HandStartGetBroker(Info_in{
		cli_name:   req.CliName,
		topic_name: req.TopicName,
		part_name:  req.PartName,
		option:     req.Option,
		index:      req.Index,
	})
	if err != nil {
		return &api.ConStartGetBrokResponse{
			Ret: false,
		}, err
	}
	return &api.ConStartGetBrokResponse{
		Ret:   true,
		Size:  int64(size),
		Parts: parts,
	}, nil
}

//func (s *RPCServer) ConGetBroker(ctx context.Context, req *api.ConGetBrokRequest) (r *api.ConGetBrokResponse, err error) {
//
//}

// broker---->zkserver
// broker连接到zkserver后会立即发送info，让zkserver连接到broker
func (s *RPCServer) BroInfo(ctx context.Context, req *api.BroInfoRequest) (r *api.BroInfoResponse, err error) {
	err = s.zkserver.HandleBroInfo(req.BrokerName, req.BrokerHostPort)
	if err != nil {
		DEBUG(dError, err.Error())
		return &api.BroInfoResponse{
			Ret: false,
		}, err
	}
	return &api.BroInfoResponse{
		Ret: true,
	}, nil
}

// broker---->zkserver
func (s *RPCServer) BroGetConfig(ctx context.Context, req *api.BroGetConfigRequest) (r *api.BroGetConfigResponse, err error) {
	/*
		用于broker加载缓存
		......
		暂时不开启这个功能，有待考虑它的必要性
	*/

	return &api.BroGetConfigResponse{
		Ret: true,
	}, nil
}

// consumer---->zkserver
// 订阅
func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (resp *api.SubResponse, err error) {

	err = s.zkserver.SubHandle(info_in{
		cli_name:   req.Consumer,
		topic_name: req.Topic,
		part_name:  req.Key,
		option:     req.Option,
	})

	if err == nil {
		return &api.SubResponse{
			Ret: true,
		}, nil
	}

	return &api.SubResponse{Ret: false}, err
}

// zkserver---->broker server
func (s *RPCServer) PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest) (r *api.PrepareAcceptResponse, err error) {
	ret, err := s.server.PrepareAcceptHandle(info{
		topic_name: req.TopicName,
		part_name:  req.PartName,
		file_name:  req.FileName,
	})
	if err != nil {
		return &api.PrepareAcceptResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.PrepareAcceptResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// zkserver---->broker server
// zkserver控制broker停止接收某个partition的信息，
// 并修改文件名，关闭partition中的fd等
func (s *RPCServer) CloseAccept(ctx context.Context, req *api.CloseAcceptRequest) (r *api.CloseAcceptResponse, err error) {

}

// zkserver---->broker server
func (s *RPCServer) PrepareSend(ctx context.Context, req *api.PrepareSendRequest) (r *api.PrepareSendResponse, err error) {
	ret, err := s.server.PrepareSendHandle(info{
		topic_name: req.TopicName,
		part_name:  req.PartName,
		file_name:  req.FileName,
		option:     req.Option,
		offset:     req.Offset,
	})
	if err != nil {
		return &api.PrepareSendResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.PrepareSendResponse{
		Ret: true,
		Err: ret,
	}, nil
}
