// Code generated by Kitex v0.10.1. DO NOT EDIT.

package raft_operations

import (
	"context"
	"errors"
	client "github.com/cloudwego/kitex/client"
	kitex "github.com/cloudwego/kitex/pkg/serviceinfo"
	api "zhuMQ/kitex_gen/api"
)

var errInvalidMessageType = errors.New("invalid message type for service method handler")

var serviceMethods = map[string]kitex.MethodInfo{
	"RequestVote": kitex.NewMethodInfo(
		requestVoteHandler,
		newRaft_OperationsRequestVoteArgs,
		newRaft_OperationsRequestVoteResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"AppendEntries": kitex.NewMethodInfo(
		appendEntriesHandler,
		newRaft_OperationsAppendEntriesArgs,
		newRaft_OperationsAppendEntriesResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"SnapShot": kitex.NewMethodInfo(
		snapShotHandler,
		newRaft_OperationsSnapShotArgs,
		newRaft_OperationsSnapShotResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
}

var (
	raft_OperationsServiceInfo                = NewServiceInfo()
	raft_OperationsServiceInfoForClient       = NewServiceInfoForClient()
	raft_OperationsServiceInfoForStreamClient = NewServiceInfoForStreamClient()
)

// for server
func serviceInfo() *kitex.ServiceInfo {
	return raft_OperationsServiceInfo
}

// for stream client
func serviceInfoForStreamClient() *kitex.ServiceInfo {
	return raft_OperationsServiceInfoForStreamClient
}

// for client
func serviceInfoForClient() *kitex.ServiceInfo {
	return raft_OperationsServiceInfoForClient
}

// NewServiceInfo creates a new ServiceInfo containing all methods
func NewServiceInfo() *kitex.ServiceInfo {
	return newServiceInfo(false, true, true)
}

// NewServiceInfo creates a new ServiceInfo containing non-streaming methods
func NewServiceInfoForClient() *kitex.ServiceInfo {
	return newServiceInfo(false, false, true)
}
func NewServiceInfoForStreamClient() *kitex.ServiceInfo {
	return newServiceInfo(true, true, false)
}

func newServiceInfo(hasStreaming bool, keepStreamingMethods bool, keepNonStreamingMethods bool) *kitex.ServiceInfo {
	serviceName := "Raft_Operations"
	handlerType := (*api.Raft_Operations)(nil)
	methods := map[string]kitex.MethodInfo{}
	for name, m := range serviceMethods {
		if m.IsStreaming() && !keepStreamingMethods {
			continue
		}
		if !m.IsStreaming() && !keepNonStreamingMethods {
			continue
		}
		methods[name] = m
	}
	extra := map[string]interface{}{
		"PackageName": "api",
	}
	if hasStreaming {
		extra["streaming"] = hasStreaming
	}
	svcInfo := &kitex.ServiceInfo{
		ServiceName:     serviceName,
		HandlerType:     handlerType,
		Methods:         methods,
		PayloadCodec:    kitex.Thrift,
		KiteXGenVersion: "v0.10.1",
		Extra:           extra,
	}
	return svcInfo
}

func requestVoteHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Raft_OperationsRequestVoteArgs)
	realResult := result.(*api.Raft_OperationsRequestVoteResult)
	success, err := handler.(api.Raft_Operations).RequestVote(ctx, realArg.Rep)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newRaft_OperationsRequestVoteArgs() interface{} {
	return api.NewRaft_OperationsRequestVoteArgs()
}

func newRaft_OperationsRequestVoteResult() interface{} {
	return api.NewRaft_OperationsRequestVoteResult()
}

func appendEntriesHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Raft_OperationsAppendEntriesArgs)
	realResult := result.(*api.Raft_OperationsAppendEntriesResult)
	success, err := handler.(api.Raft_Operations).AppendEntries(ctx, realArg.Rep)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newRaft_OperationsAppendEntriesArgs() interface{} {
	return api.NewRaft_OperationsAppendEntriesArgs()
}

func newRaft_OperationsAppendEntriesResult() interface{} {
	return api.NewRaft_OperationsAppendEntriesResult()
}

func snapShotHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Raft_OperationsSnapShotArgs)
	realResult := result.(*api.Raft_OperationsSnapShotResult)
	success, err := handler.(api.Raft_Operations).SnapShot(ctx, realArg.Rep)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newRaft_OperationsSnapShotArgs() interface{} {
	return api.NewRaft_OperationsSnapShotArgs()
}

func newRaft_OperationsSnapShotResult() interface{} {
	return api.NewRaft_OperationsSnapShotResult()
}

type kClient struct {
	c client.Client
}

func newServiceClient(c client.Client) *kClient {
	return &kClient{
		c: c,
	}
}

func (p *kClient) RequestVote(ctx context.Context, rep *api.RequestVoteArgs_) (r *api.RequestVoteReply, err error) {
	var _args api.Raft_OperationsRequestVoteArgs
	_args.Rep = rep
	var _result api.Raft_OperationsRequestVoteResult
	if err = p.c.Call(ctx, "RequestVote", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) AppendEntries(ctx context.Context, rep *api.AppendEntriesArgs_) (r *api.AppendEntriesReply, err error) {
	var _args api.Raft_OperationsAppendEntriesArgs
	_args.Rep = rep
	var _result api.Raft_OperationsAppendEntriesResult
	if err = p.c.Call(ctx, "AppendEntries", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) SnapShot(ctx context.Context, rep *api.SnapShotArgs_) (r *api.SnapShotReply, err error) {
	var _args api.Raft_OperationsSnapShotArgs
	_args.Rep = rep
	var _result api.Raft_OperationsSnapShotResult
	if err = p.c.Call(ctx, "SnapShot", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}
