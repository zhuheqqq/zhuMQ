package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cloudwego/kitex/pkg/registry"
	"github.com/go-zookeeper/zk"
	"github.com/kitex-contrib/registry-zookeeper/entity"
	"github.com/kitex-contrib/registry-zookeeper/utils"
	"net"
	"strings"
	"time"
)

// 保存zookeeper连接信息及认证信息
type zookeeperRegistry struct {
	conn     *zk.Conn
	authOpen bool
	user     string
	password string
}

// 创建一个不带认证的zookeeper注册中心实例
func NewZookeeperRegistry(servers []string, sessionTimeout time.Duration) (registry.Registry, error) {
	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	return &zookeeperRegistry{conn: conn}, nil
}

// 创建一个带认证的zookeeper注册中心实例
func NewZookeeperRegistryWithAuth(servers []string, sessionTimeout time.Duration, user, password string) (registry.Registry, error) {
	if user == "" || password == "" {
		return nil, fmt.Errorf("user or password is empty")
	}
	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	auth := []byte(fmt.Sprintf("%s:%s", user, password))
	err = conn.AddAuth(utils.Scheme, auth)
	if err != nil {
		return nil, err
	}
	return &zookeeperRegistry{conn: conn, authOpen: true, user: user, password: password}, nil
}

// 注册服务信息
func (z *zookeeperRegistry) Register(info *registry.Info) error {
	path, err := buildPath(info)
	if err != nil {
		return err
	}
	content, err := json.Marshal(&entity.RegistryEntity{Weight: info.Weight, Tags: info.Tags})
	if err != nil {
		return err
	}
	return z.createNode(path, content, true)
}

// path format as follows:
// /{serviceName}/{ip}:{port}
func buildPath(info *registry.Info) (string, error) {
	var path string
	if info == nil {
		return "", fmt.Errorf("registry info can't be nil")
	}
	if info.ServiceName == "" {
		return "", fmt.Errorf("registry info service name can't be empty")
	}
	if info.Addr == nil {
		return "", fmt.Errorf("registry info addr can't be nil")
	}
	if !strings.HasPrefix(info.ServiceName, utils.Separator) {
		path = utils.Separator + info.ServiceName
	}

	if host, port, err := net.SplitHostPort(info.Addr.String()); err == nil {
		if port == "" {
			return "", fmt.Errorf("registry info addr missing port")
		}
		if host == "" {
			ipv4, err := utils.GetLocalIPv4Address()
			if err != nil {
				return "", fmt.Errorf("get local ipv4 error, cause %w", err)
			}
			path = path + utils.Separator + ipv4 + ":" + port
		} else {
			path = path + utils.Separator + host + ":" + port
		}
	} else {
		return "", fmt.Errorf("parse registry info addr error")
	}
	return path, nil
}

// 注销服务消息
func (z *zookeeperRegistry) Deregister(info *registry.Info) error {
	if info == nil {
		return fmt.Errorf("registry info can't be nil")
	}
	path, err := buildPath(info)
	if err != nil {
		return err
	}
	return z.deleteNode(path)
}

// 在zookeeper中创建节点
func (z *zookeeperRegistry) createNode(path string, content []byte, ephemeral bool) error {
	i := strings.LastIndex(path, utils.Separator)
	if i > 0 {
		err := z.createNode(path[0:i], nil, false)
		if err != nil && !errors.Is(err, zk.ErrNodeExists) {
			return err
		}
	}
	var flag int32
	if ephemeral {
		flag = zk.FlagEphemeral
	}
	if z.authOpen {
		_, err := z.conn.Create(path, content, flag, zk.DigestACL(zk.PermAll, z.user, z.password))
		if err != nil {
			return fmt.Errorf("create node [%s] with auth error, cause %w", path, err)
		}
		return nil
	} else {
		_, err := z.conn.Create(path, content, flag, zk.WorldACL(zk.PermAll))
		if err != nil {
			return fmt.Errorf("create node [%s] error, cause %w", path, err)
		}
		return nil
	}
}

func (z *zookeeperRegistry) deleteNode(path string) error {
	err := z.conn.Delete(path, -1)
	if err != nil {
		return fmt.Errorf("delete node [%s] error, cause %w", path, err)
	}
	return nil
}
