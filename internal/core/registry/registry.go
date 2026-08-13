package registry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// DefaultPrefix etcd 中服务 key 的前缀
const DefaultPrefix = "/kamaRPC/services/"

// Instance 一个服务实例, 演示版只保留地址
type Instance struct {
	Addr string
}

// Registry 基于 etcd 的注册中心封装: 注册(服务端) + 发现(客户端)
// 发现侧采用 "首次 Get 全量 + 本地缓存 + watch 增量更新", 避免每次调用都打 etcd
type Registry struct {
	client *clientv3.Client
	prefix string

	mu       sync.RWMutex
	services map[string]map[string]Instance // serviceName -> (addr -> Instance)

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRegistry 连接 etcd
func NewRegistry(endpoints []string) (*Registry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Registry{
		client:   cli,
		prefix:   DefaultPrefix,
		services: make(map[string]map[string]Instance),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Register 注册实例并持续续租, 服务进程挂掉后 key 会随租约过期自动删除
func (r *Registry) Register(service string, ins Instance, ttl int64) error {
	// 申请带过期时间的租约
	leaseResp, err := r.client.Grant(r.ctx, ttl)
	if err != nil {
		return err
	}

	// key 形如 /kamaRPC/services/Arith/localhost:9090
	key := fmt.Sprintf("%s%s/%s", r.prefix, service, ins.Addr)

	if _, err = r.client.Put(r.ctx, key, ins.Addr, clientv3.WithLease(leaseResp.ID)); err != nil {
		return err
	}

	// 心跳续租, 相当于自动健康检查
	ch, err := r.client.KeepAlive(r.ctx, leaseResp.ID)
	if err != nil {
		return err
	}

	go func() {
		for {
			if _, ok := <-ch; !ok {
				return
			}
		}
	}()

	return nil
}

// Deregister 主动摘除实例
func (r *Registry) Deregister(service string, ins Instance) error {
	key := fmt.Sprintf("%s%s/%s", r.prefix, service, ins.Addr)
	_, err := r.client.Delete(r.ctx, key)
	return err
}

// Discover 查询服务实例, 命中缓存直接返回
func (r *Registry) Discover(service string) ([]Instance, error) {
	r.mu.RLock()
	if _, ok := r.services[service]; ok {
		r.mu.RUnlock()
		return r.copyInstances(service), nil
	}
	r.mu.RUnlock()

	// 第一次发现, 初始化缓存并启动 watch
	if err := r.initService(service); err != nil {
		return nil, err
	}

	return r.copyInstances(service), nil
}

// initService 从 etcd 拉取全量实例写入缓存, 并启动 watch
func (r *Registry) initService(service string) error {
	r.mu.Lock()

	// 防止重复初始化
	if _, ok := r.services[service]; ok {
		r.mu.Unlock()
		return nil
	}

	key := fmt.Sprintf("%s%s/", r.prefix, service)

	resp, err := r.client.Get(r.ctx, key, clientv3.WithPrefix())
	if err != nil {
		r.mu.Unlock()
		return err
	}

	r.services[service] = make(map[string]Instance)
	for _, kv := range resp.Kvs {
		addr := string(kv.Value)
		r.services[service][addr] = Instance{Addr: addr}
	}
	r.mu.Unlock()

	go r.watch(service)

	return nil
}

func (r *Registry) copyInstances(service string) []Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()

	instances := make([]Instance, 0, len(r.services[service]))
	for _, ins := range r.services[service] {
		instances = append(instances, ins)
	}
	return instances
}

// watch 监听前缀变化, 实时维护本地缓存
func (r *Registry) watch(service string) {
	key := fmt.Sprintf("%s%s/", r.prefix, service)

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		watchChan := r.client.Watch(r.ctx, key, clientv3.WithPrefix())

		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				switch event.Type {

				case clientv3.EventTypePut:
					addr := string(event.Kv.Value)
					r.mu.Lock()
					r.services[service][addr] = Instance{Addr: addr}
					r.mu.Unlock()

				case clientv3.EventTypeDelete:
					deletedKey := string(event.Kv.Key)
					addr := strings.TrimPrefix(deletedKey, r.prefix+service+"/")
					r.mu.Lock()
					delete(r.services[service], addr)
					r.mu.Unlock()
				}
			}
		}

		// watch 断了, 稍后重连
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// Close 关闭 watch/keepalive 等后台协程与 etcd 连接
func (r *Registry) Close() error {
	r.cancel()
	return r.client.Close()
}
