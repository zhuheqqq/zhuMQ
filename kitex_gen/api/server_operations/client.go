// Code generated by Kitex v0.10.1. DO NOT EDIT.

package server_operations

import (
	"context"
	client "github.com/cloudwego/kitex/client"
	callopt "github.com/cloudwego/kitex/client/callopt"
	api "zhuMQ/kitex_gen/api"
)

// Client is designed to provide IDL-compatible methods with call-option parameter for kitex framework.
type Client interface {
	Push(ctx context.Context, req *api.PushRequest, callOptions ...callopt.Option) (r *api.PushResponse, err error)
	Pull(ctx context.Context, req *api.PullRequest, callOptions ...callopt.Option) (r *api.PullResponse, err error)
	ConInfo(ctx context.Context, req *api.InfoRequest, callOptions ...callopt.Option) (r *api.InfoResponse, err error)
	StarttoGet(ctx context.Context, req *api.InfoGetRequest, callOptions ...callopt.Option) (r *api.InfoGetResponse, err error)
	PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest, callOptions ...callopt.Option) (r *api.PrepareAcceptResponse, err error)
	PrepareSend(ctx context.Context, req *api.PrepareSendRequest, callOptions ...callopt.Option) (r *api.PrepareSendResponse, err error)
}

// NewClient creates a client for the service defined in IDL.
func NewClient(destService string, opts ...client.Option) (Client, error) {
	var options []client.Option
	options = append(options, client.WithDestService(destService))

	options = append(options, opts...)

	kc, err := client.NewClient(serviceInfoForClient(), options...)
	if err != nil {
		return nil, err
	}
	return &kServer_OperationsClient{
		kClient: newServiceClient(kc),
	}, nil
}

// MustNewClient creates a client for the service defined in IDL. It panics if any error occurs.
func MustNewClient(destService string, opts ...client.Option) Client {
	kc, err := NewClient(destService, opts...)
	if err != nil {
		panic(err)
	}
	return kc
}

type kServer_OperationsClient struct {
	*kClient
}

func (p *kServer_OperationsClient) Push(ctx context.Context, req *api.PushRequest, callOptions ...callopt.Option) (r *api.PushResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.Push(ctx, req)
}

func (p *kServer_OperationsClient) Pull(ctx context.Context, req *api.PullRequest, callOptions ...callopt.Option) (r *api.PullResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.Pull(ctx, req)
}

func (p *kServer_OperationsClient) ConInfo(ctx context.Context, req *api.InfoRequest, callOptions ...callopt.Option) (r *api.InfoResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.ConInfo(ctx, req)
}

func (p *kServer_OperationsClient) StarttoGet(ctx context.Context, req *api.InfoGetRequest, callOptions ...callopt.Option) (r *api.InfoGetResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.StarttoGet(ctx, req)
}

func (p *kServer_OperationsClient) PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest, callOptions ...callopt.Option) (r *api.PrepareAcceptResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.PrepareAccept(ctx, req)
}

func (p *kServer_OperationsClient) PrepareSend(ctx context.Context, req *api.PrepareSendRequest, callOptions ...callopt.Option) (r *api.PrepareSendResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.PrepareSend(ctx, req)
}
