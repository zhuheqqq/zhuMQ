package main

import (
	"fmt"
	"github.com/cloudwego/kitex/server"
	"net"
	Server "zhuMQ/server"
)

func main() {
	addr, _ := net.ResolveTCPAddr("tcp", ":8888")
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	rpcServer := new(Server.RPCServer)

	err := rpcServer.Start(opts)
	if err != nil {
		fmt.Println(err)
	}
}
