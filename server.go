package gyyrpc

import (
	"encoding/json"
	"fmt"
	"gyyrpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

// rpc 报文格式为：| Option | Header1 | Body1 | Header2 | Body2 | ...

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber int        // 标记 gyyRPC 请求
	CodecType   codec.Type // 编码类型
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

// Server RPC 服务器
type Server struct{}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

// ServeConn
// 1. 解析并验证 option 信息，获取请求的编解码器类型
// 2. 解析请求其余部分 header | body | header | body ...
func (s *Server) ServeConn(conn io.ReadWriteCloser) {
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
	s.serveCodec(f(conn))
}

// invalidRequest 发生错误时，作为 argv 占位符
var invalidRequest = struct{}{}

// serveCodec
// 循环做："读取请求 -> 处理请求 -> 回复处理"，直到
func (s *Server) serveCodec(cc codec.Codec) {
	sending := sync.Mutex{}
	wg := sync.WaitGroup{}
	// 报文格式是 option | head1 | body1 | head2 | body2 | .. 所以不断循环读取 head 和 body
	for {
		// 读取一次请求中的 header 和 body
		req, err := s.readRequest(cc)
		if err != nil {
			// 可能读取 header 的时候就出问题了，此时 req 还未初始化
			if req == nil {
				break
			}
			// 封装错误，返回给 client
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, &sending)
			continue
		}
		wg.Add(1)
		// 根据 request 传入的信息做实际处理
		go s.handleRequest(cc, req, &sending, &wg)
	}
	wg.Wait()
	cc.Close()
}

type request struct {
	h      *codec.Header // request 的 header 信息
	argv   reflect.Value // 请求参数
	replyv reflect.Value // 返回参数
}

// 解析 Header 信息
func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error: ", err)
		}
		return nil, err
	}
	return &h, nil
}

func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	// 解析 header 信息
	h, err := s.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	// 根据 argv 数据类型进行解析
	// 这里将 argv 简单假设为字符串处理
	req.argv = reflect.New(reflect.TypeOf(""))
	// 解析 body 信息
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

// sendResponse
// 回复客户端信息
func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	// 调用注册的 rpc 方法去获取返回值
	// 这里简单返回一个字符串
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("gyyrpc resp %d", req.h.Seq))
	s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}

func (s *Server) Accept(list net.Listener) {
	// 循环：接收请求，执行操作的过程
	for {
		conn, err := list.Accept()
		if err != nil {
			log.Println("rcp server: accept error:", err)
			return
		}
		go s.ServeConn(conn)
	}
}

func Accept(list net.Listener) {
	// 使用默认 server 对象，方便用户调用
	DefaultServer.Accept(list)
}
