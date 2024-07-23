package server

import "zhuMQ/kitex_gen/api/client_operations"

type Group struct {
	topics    []*Topic
	consumers map[string]*client_operations.Client
}
