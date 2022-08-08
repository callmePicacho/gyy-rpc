package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

// GyyRegistryDiscovery 基于注册中心的服务发现
type GyyRegistryDiscovery struct {
	*MultiServersDiscovery
	registry   string        // 注册中心地址
	timeout    time.Duration // 服务列表的过期时间
	lastUpdate time.Time     // 最后从注册中心更新服务列表的时间，默认 10 s 过期
}

const defaultUpdateTimeout = time.Second * 10

func NewGyyRegistryDiscovery(registerAddr string, timeout time.Duration) *GyyRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}

	d := &GyyRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:              registerAddr,
		timeout:               timeout,
	}
	return d
}

func (d *GyyRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *GyyRegistryDiscovery) Refresh() error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 最后更新时间 + 过期时间，如果还没到这个时间点，直接返回不必更新
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)

	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-Gyyrpc-Servers"), ", ")
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.servers = append(d.servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *GyyRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", nil
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *GyyRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, nil
	}
	return d.MultiServersDiscovery.GetAll()
}
