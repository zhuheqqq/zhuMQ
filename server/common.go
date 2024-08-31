package server

import (
	Ser "github.com/cloudwego/kitex/server"
	"net"
	"os"
	"runtime"
	"zhuMQ/kitex_gen/api/client_operations"
	"zhuMQ/logger"
	"zhuMQ/zookeeper"
)

type PartKey struct {
	Name string `json:"name"`
}

type Parts struct {
	PartKeys []PartKey `json:"partkeys"`
}

// 初始化Broker时的信息
type Options struct {
	Name               string
	Tag                string
	Zkserver_Host_Port string
	Broker_Host_Port   string
	Raft_Host_Port     string
	Me                 int
}

// broker向zookeeper发送自己的新能指标，用于按权值负载均衡
type Property struct {
	Name    string `json:"name"`
	Power   int64  `json:"power"`
	CPURate int64  `json:"cpurate"`
	DiskIO  int64  `json:"diskio"`
}

type BroNodeInfo struct {
	Topics map[string]TopNodeInfo `json:"topics"`
}

type TopNodeInfo struct {
	Topic_name string
	Part_nums  int
	Partitions map[string]PartNodeInfo
}

type PartNodeInfo struct {
	Part_name  string
	Block_name int
	Blocks     map[string]BloNodeInfo
}

type BloNodeInfo struct {
	Start_index int64
	End_index   int64
	Path        string
	File_name   string
}

type BrokerS struct {
	BroBrokers map[string]string `json:"brobrokers"`
	RafBrokers map[string]string `json:"rafbrokers"`
	Me_Brokers map[string]int    `json:"mebrokers"`
}

const (
	ZKBROKER = "zkbroker"
	BROKER   = "broker"
)

func NewBrokerAndStart(zkinfo zookeeper.ZKInfo, opt Options) *RPCServer {
	//start the broker server
	// fmt.Println("Broker_host_Poet", opt.Broker_Host_Port)
	addr_bro, _ := net.ResolveTCPAddr("tcp", opt.Broker_Host_Port)
	addr_raf, _ := net.ResolveTCPAddr("tcp", opt.Raft_Host_Port)
	var opts_bro, opts_raf []Ser.Option
	opts_bro = append(opts_bro, Ser.WithServiceAddr(addr_bro))
	opts_raf = append(opts_raf, Ser.WithServiceAddr(addr_raf))

	rpcServer := NewRpcServer(zkinfo)

	go func() {
		err := rpcServer.Start(opts_bro, nil, opts_raf, opt)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err)
		}
	}()

	return &rpcServer
}

func NewZKServerAndStart(zkinfo zookeeper.ZKInfo, opt Options) *RPCServer {
	//start the zookeeper server
	addr_zks, _ := net.ResolveTCPAddr("tcp", opt.Zkserver_Host_Port)
	var opts_zks []Ser.Option
	opts_zks = append(opts_zks, Ser.WithServiceAddr(addr_zks))

	rpcServer := NewRpcServer(zkinfo)

	go func() {
		err := rpcServer.Start(nil, opts_zks, nil, opt)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err)
		}
	}()

	return &rpcServer
}

func GetIpport() string {
	interfaces, err := net.Interfaces()
	ipport := ""
	if err != nil {
		panic("Poor soul, here is what you got:" + err.Error())
	}
	for _, inter := range interfaces {
		mac := inter.HardwareAddr //获取本机MAC地址
		ipport += mac.String()
	}
	return ipport
}

func GetBlockName(file_name string) (ret string) {
	ret = file_name[:len(file_name)-4]
	return ret
}

func CheckFileOrList(path string) (ret bool) {
	_, err := os.Stat(path)
	if err != nil {
		return os.IsExist(err)
	}
	ret = true
	return ret
}

func CreateList(path string) error {
	ret := CheckFileOrList(path)

	if !ret {
		err := os.Mkdir(path, 0775)
		if err != nil {
			_, file, line, _ := runtime.Caller(1)
			logger.DEBUG(logger.DError, "%v:%v mkdir %v error %v", file, line, path, err.Error())
		}
	}
	return nil
}

func CreateFile(path string) (file *os.File, err error) {
	file, err = os.Create(path)
	return file, err
}

func CheckChangeCli(old map[string]*client_operations.Client, new []string) (reduce, add []string) {
	for _, new_cli := range new {
		if _, ok := old[new_cli]; !ok { //new_cli 在old中没有
			add = append(add, new_cli)
		}
	}

	for old_cli := range old {
		had := false //不存在
		for _, name := range new {
			if old_cli == name {
				had = true
				break
			}
		}
		if !had {
			reduce = append(reduce, old_cli)
		}
	}

	return reduce, add
}

func GetPartKeyArray(parts map[string]*Partition) []PartKey {
	var array []PartKey
	for part_name := range parts {
		array = append(array, PartKey{
			Name: part_name,
		})
	}
	return array
}

func MovName(OldFilePath, NewFilePath string) error {
	return os.Rename(OldFilePath, NewFilePath)
}

// use for Test
type Info struct {
	// name       string //broker name
	Topic_name string
	Part_name  string
	File_name  string
	New_name   string
	Option     int8
	Offset     int64
	Size       int8

	Ack int8

	Producer string
	Consumer string
	Cmdindex int64
	// startIndex  int64
	// endIndex  	int64
	Message []byte

	//raft
	Brokers map[string]string
	Me      int

	//fetch
	LeaderBroker string
	HostPort     string
}

// use in test
func GetInfo(in Info) info {
	return info{
		topic_name:   in.Topic_name,
		part_name:    in.Part_name,
		file_name:    in.File_name,
		new_name:     in.New_name,
		option:       in.Option,
		offset:       in.Offset,
		size:         in.Size,
		ack:          in.Ack,
		producer:     in.Producer,
		consumer:     in.Consumer,
		cmdindex:     in.Cmdindex,
		message:      in.Message,
		brokers:      in.Brokers,
		me:           in.Me,
		LeaderBroker: in.LeaderBroker,
		HostPort:     in.HostPort,
	}
}

func GetServerInfoAply() chan info {
	return make(chan info)
}
