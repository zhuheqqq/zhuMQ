package main

import (
	"context"
	"fmt"
	client2 "github.com/cloudwego/kitex/client"
	"time"
	client3 "zhuMQ/client/client"
	"zhuMQ/kitex_gen/api"
	"zhuMQ/kitex_gen/api/server_operations"
)

func main() {

	consumer := client3.Consumer{}

	//预留代码
	if 1 == 2 {
		//consumer
		//start a server for pub and pingpong
		consumer = client3.Consumer{}
		go consumer.Start_server(":8889")
	}

	//connection the broker server for push/pull/info
	client, err := server_operations.NewClient("client", client2.WithHostPorts("0.0.0.0:8888"))
	if err != nil {
		fmt.Println(err)
	}
	consumer.Cli = client

	//send ip and port for brokerserver can pub this client
	info := &api.InfoRequest{
		IpPort: "0.0.0.0:8889",
	}
	resp, err := client.Info(context.Background(), info)
	if err != nil {
		fmt.Println(resp)
	}

	//test
	for {
		req := &api.PushRequest{
			Producer: int64(1),
			Topic:    "phone number",
			Key:      "zhuheqqq",
			Message:  "18788888888",
		}
		resp, err := client.Push(context.Background(), req)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(resp)
		time.Sleep(5 * time.Second)
	}
}
