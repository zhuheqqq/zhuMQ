package client

import "zhuMQ/kitex_gen/api/server_operations"

type Producer struct {
	Cli server_operations.Client
}
