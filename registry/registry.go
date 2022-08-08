package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// GyyRegister 注册中心，简单提供以下功能
// 注册服务实例并且接收心跳保活
// 向客户端返回当前所有可用服务器，并且删除已过期服务器
type GyyRegister struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

// ServerItem 注册的服务实例
type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_gyyrpc_/register"
	defaultTimeout = time.Minute * 5 // 默认 5 分钟超时，即注册时间超过 5 min，视为不可用
)

func New(timeout time.Duration) *GyyRegister {
	return &GyyRegister{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultGyyRegister = New(defaultTimeout)

// putServer 添加服务实例，如果服务已存在，更新 start
func (r *GyyRegister) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		s.start = time.Now()
	}
}

// aliveServers 返回可用服务，删除超时服务
func (r *GyyRegister) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var alive []string
	for addr, s := range r.servers {
		// 如果没有设置超时时间，或者超时时间内，添加到可用列表
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			// 否则超时了，直接删除
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// ServeHTTP 路由为 /_gyyrpc_/register
func (r *GyyRegister) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("X-Gyyrpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-Gyyrpc-Servers")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *GyyRegister) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultGyyRegister.HandleHTTP(defaultPath)
}

// Heartbeat 每隔一段时间发送一次心跳
func Heartbeat(register, addr string, duration time.Duration) {
	if duration == 0 {
		// 比默认过期时间短一分钟
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(register, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(register, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Gyyrpc-Servers", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
