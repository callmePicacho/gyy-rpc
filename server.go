package gyyrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"gyyrpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

// rpc 报文格式为：| Option | Header1 | Body1 | Header2 | Body2 | ...

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber    int           // 标记 gyyRPC 请求
	CodecType      codec.Type    // 编码类型
	ConnectTimeout time.Duration // 连接超时时间
	HandlerTimeout time.Duration // 处理超时时间
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10, // 默认 10s
}

// Server RPC 服务器
type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

// ServeConn
// 1. 解析并验证 option 信息，获取请求的编解码器类型
// 2. 解析请求其余部分 header | body | header | body ...
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		conn.Close()
	}()
	var opt Option
	// 使用 JSON 解析器解析 opt
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	// 验证 opt
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	// 根据 opt 中获取的编码类型，调用合适的构造函数初始化获取实例送入 serveCodec
	server.serveCodec(f(conn), &opt)
}

// invalidRequest 发生错误时，作为 argv 占位符
var invalidRequest = struct{}{}

// serveCodec
// 循环做："读取请求 -> 处理请求 -> 回复处理"，直到
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := sync.Mutex{}
	wg := sync.WaitGroup{}
	// 报文格式是 option | head1 | body1 | head2 | body2 | .. 所以不断循环读取 head 和 body
	for {
		// 读取一次请求中的 header 和 body
		req, err := server.readRequest(cc)
		if err != nil {
			// 可能读取 header 的时候就出问题了，此时 req 还未初始化
			if req == nil {
				break
			}
			// 封装错误，返回给 client
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, &sending)
			continue
		}
		wg.Add(1)
		// 根据 request 传入的信息做实际处理
		go server.handleRequest(cc, req, &sending, &wg, opt.HandlerTimeout)
	}
	wg.Wait()
	cc.Close()
}

type request struct {
	h      *codec.Header // request 的 header 信息
	argv   reflect.Value // 请求参数
	replyv reflect.Value // 返回参数
	mtype  *methodType
	svc    *service
}

// 解析 Header 信息
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error: ", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	// 解析 header 信息
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	// 根据 header 中带的"服务名.方法名"，找到对应的服务和方法
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	// 获取注册的 RPC 方法参数同类型零值实例
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// 确保 argv 是个指针类型，因为 ReadBody 需要指针类型作为参数
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}

	// 解析 body 信息
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv err:", err)
		return req, err
	}
	return req, nil
}

// sendResponse
// 回复客户端信息
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{}) // 控制调用
	sent := make(chan struct{})   // 控制服务端发送
	go func() {
		// 调用注册的 rpc 方法获取返回值
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	// 超时时间为0，立即返回
	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
	case <-called:
		<-sent
	}
}

func (server *Server) Accept(list net.Listener) {
	// 循环：接收请求，执行操作的过程
	for {
		conn, err := list.Accept()
		if err != nil {
			log.Println("rcp server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

func Accept(list net.Listener) {
	// 使用默认 server 对象，方便用户调用
	DefaultServer.Accept(list)
}

// Register 将 rcvr 作为接收者满足以下条件的方法注册到服务器：
// - 方法和其类型都支持导出
// - 有两个参数，且都是可以导出的类型
// - 第二个参数是指针类型
// - 有一个返回值，且为 error 类型
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service alread defined: " + s.name)
	}
	return nil
}

func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

// findService 通过 ServiceMethod 从 serviceMap 中找到对应的 service
// serviceMethod 格式："服务名.方法名"
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	// 找服务
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	// 找方法
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}
