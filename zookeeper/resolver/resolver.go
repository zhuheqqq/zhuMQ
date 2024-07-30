package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/go-zookeeper/zk"
	"github.com/kitex-contrib/registry-zookeeper/entity"
	"github.com/kitex-contrib/registry-zookeeper/utils"
	"net"
	"strings"
	"time"
)

type zookeeperResolver struct {
	conn *zk.Conn
}

func NewZookeeperResolver(servers []string, sessionTimeout time.Duration) (discovery.Resolver, error) {
	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	return &zookeeperResolver{conn: conn}, nil
}

func NewZookeeperResolverWithAuth(servers []string, sessionTimeout time.Duration, user, password string) (discovery.Resolver, error) {
	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	auth := []byte(fmt.Sprintf("%s:%s", user, password))
	err = conn.AddAuth(utils.Scheme, auth)
	if err != nil {
		return nil, err
	}
	return &zookeeperResolver{conn: conn}, nil
}

// Target implements the Resolver interface.
// 返回服务的目标，即服务名
func (r *zookeeperResolver) Target(ctx context.Context, target rpcinfo.EndpointInfo) string {
	return target.ServiceName()
}

// Resolve implements the Resolver interface.
// 解析服务名，查找服务实例
func (r *zookeeperResolver) Resolve(ctx context.Context, desc string) (discovery.Result, error) {
	path := desc
	if !strings.HasPrefix(path, utils.Separator) {
		path = utils.Separator + path
	}
	eps, err := r.getEndPoints(path)
	if err != nil {
		return discovery.Result{}, err
	}
	if len(eps) == 0 {
		return discovery.Result{}, fmt.Errorf("no instance remains for %v", desc)
	}
	instances, err := r.getInstances(eps, path)
	if err != nil {
		return discovery.Result{}, err
	}
	res := discovery.Result{
		Cacheable: true,
		CacheKey:  desc,
		Instances: instances,
	}
	return res, nil
}

// 获取指定路径的所有子节点
func (r *zookeeperResolver) getEndPoints(path string) ([]string, error) {
	child, _, err := r.conn.Children(path)
	return child, err
}

// 获取子节点详细信息
func (r *zookeeperResolver) detailEndPoints(path, ep string) (discovery.Instance, error) {
	data, _, err := r.conn.Get(path + utils.Separator + ep)
	if err != nil {
		return nil, err
	}
	en := new(entity.RegistryEntity)
	err = json.Unmarshal(data, en)
	if err != nil {
		return nil, fmt.Errorf("unmarshal data [%s] error, cause %w", data, err)
	}
	return discovery.NewInstance("tcp", ep, en.Weight, en.Tags), nil
}

// 获取所有子节点详细信息
func (r *zookeeperResolver) getInstances(eps []string, path string) ([]discovery.Instance, error) {
	instances := make([]discovery.Instance, 0, len(eps))
	for _, ep := range eps {
		if host, port, err := net.SplitHostPort(ep); err == nil {
			if port == "" {
				return []discovery.Instance{}, fmt.Errorf("missing port when parse node [%s]", ep)
			}
			if host == "" {
				return []discovery.Instance{}, fmt.Errorf("missing host when parse node [%s]", ep)
			}
			ins, err := r.detailEndPoints(path, ep)
			if err != nil {
				return []discovery.Instance{}, fmt.Errorf("detail endpoint [%s] info error, cause %w", ep, err)
			}
			instances = append(instances, ins)
		} else {
			return []discovery.Instance{}, fmt.Errorf("parse node [%s] error, details info [%w]", ep, err)
		}
	}
	return instances, nil
}

// Diff implements the Resolver interface.
// 计算前后两次解析结果的差异
func (r *zookeeperResolver) Diff(key string, prev, next discovery.Result) (discovery.Change, bool) {
	return discovery.DefaultDiff(key, prev, next)
}

// Name implements the Resolver interface.
func (r *zookeeperResolver) Name() string {
	return "zookeeper"
}
