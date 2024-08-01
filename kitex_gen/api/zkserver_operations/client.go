// Code generated by Kitex v0.10.1. DO NOT EDIT.

package zkserver_operations

import (
	"context"
	client "github.com/cloudwego/kitex/client"
	callopt "github.com/cloudwego/kitex/client/callopt"
	api "zhuMQ/kitex_gen/api"
)

// Client is designed to provide IDL-compatible methods with call-option parameter for kitex framework.
type Client interface {
	BroInfo(ctx context.Context, req *api.BroInfoRequest, callOptions ...callopt.Option) (r *api.BroInfoResponse, err error)
	ProGetBroker(ctx context.Context, req *api.ProGetBrokRequest, callOptions ...callopt.Option) (r *api.ProGetBrokResponse, err error)
	ConGetBroker(ctx context.Context, req *api.ConGetBrokRequest, callOptions ...callopt.Option) (r *api.ConGetBrokResponse, err error)
	BroGetConfig(ctx context.Context, req *api.BroGetConfigRequest, callOptions ...callopt.Option) (r *api.BroGetConfigResponse, err error)
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
	return &kZkServer_OperationsClient{
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

type kZkServer_OperationsClient struct {
	*kClient
}

func (p *kZkServer_OperationsClient) BroInfo(ctx context.Context, req *api.BroInfoRequest, callOptions ...callopt.Option) (r *api.BroInfoResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.BroInfo(ctx, req)
}

func (p *kZkServer_OperationsClient) ProGetBroker(ctx context.Context, req *api.ProGetBrokRequest, callOptions ...callopt.Option) (r *api.ProGetBrokResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.ProGetBroker(ctx, req)
}

func (p *kZkServer_OperationsClient) ConGetBroker(ctx context.Context, req *api.ConGetBrokRequest, callOptions ...callopt.Option) (r *api.ConGetBrokResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.ConGetBroker(ctx, req)
}

func (p *kZkServer_OperationsClient) BroGetConfig(ctx context.Context, req *api.BroGetConfigRequest, callOptions ...callopt.Option) (r *api.BroGetConfigResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.BroGetConfig(ctx, req)
}