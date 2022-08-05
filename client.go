package gyyrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"gyyrpc/codec"
	"io"
	"log"
	"net"
	"sync"
)

// Call 代表一次 RPC 请求
type Call struct {
	Seq           uint64
	ServiceMethod string      // 服务名和方法名，格式为："服务名.方法名"
	Args          interface{} // 参数
	Reply         interface{} // 返回值
	Error         error
	Done          chan *Call // 完成通知
}

func (call *Call) done() {
	call.Done <- call
}

// Client RPC 客户端
type Client struct {
	cc       codec.Codec // 编解码器
	opt      *Option
	sending  sync.Mutex       // 保证消息有序发送
	header   codec.Header     // 请求的消息头
	mu       sync.Mutex       //
	seq      uint64           // 每个请求的唯一编号
	pending  map[uint64]*Call // 存储未处理完的请求，key 是编号，value 是 Call 实例
	closing  bool             // 用户调用 close 后状态为 true
	shutdown bool             // 右错误发生置为 true
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.closing {
		return ErrShutdown
	}

	client.closing = true
	return client.cc.Close()
}

// IsAvailable 返回当前客户端是否正常工作
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// registerCall
// 将参数 call 添加到 client.pending 中，并更新 client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}

	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// removeCall
// 根据 seq 从 client.pending 中移除对应的 call，并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// terminateCalls
// 服务端或客户端发生错误时调用，将错误信息通知所有 client.pending 中的 call
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()

	client.mu.Lock()
	defer client.mu.Unlock()

	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// receive
// 客户端已经发送 opt 给服务端做验证，所以服务端的响应请求中不会再回发 opt 了，故接收类型为：header | body | header | body ...
// 接收请求的响应，存在三种情况：
// 1. call 不存在，可能是请求没有发送完整，或者是其他原因取消了，但服务端仍然处理了
// 2. call 存在，但服务端处理出错，即 h.Error 不为空
// 3. call 存在，服务端处理正常，则需要从 body 中读出 reply 的值
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		// 使用解码器尝试从输入流读取一点数据做解析，如果读不到 header，可能响应收错了，没必要继续进行下去
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		// 从 client 待处理请求池中删除，并返回 call 实例
		call := client.removeCall(h.Seq)
		switch {
		case call == nil: // call 在 client 这不存在了
			err = client.cc.ReadBody(nil)
		case h.Error != "": // call 还在，但是服务端处理出错
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			// 通知已完成，放行 channel
			call.done()
		default:
			// 正确返回，从流中读取调用的返回值
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// 当 err 不为空，终止全部 client.pending 中的 calls
	client.terminateCalls(err)
}

// send
func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	// 注册调用，将 call 放入 client 的待处理请求队列中
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 准备好请求的header
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 将 header 和参数写入流，发送给服务端
	if err = client.cc.Write(&client.header, call.Args); err != nil {
		// 如果写入出错，将 call 从待处理请求队列中删除
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 暴露给用户的"异步" RPC 服务调用接口
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}

	// 根据传入参数初始化 Call 实例
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// Call 暴露给用户的"同步" RPC 服务接口
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	// 根据 opt 中编解码器类型获取相应类型构造函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	// 使用 json 格式将 opt 内容序列化，并发送给服务端做验证
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error:", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	// 使用 opt 中带的编解码器格式类型和 opt，初始化 client
	client := &Client{
		seq:     1,
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	// 同时启动子协程准备接收响应（对于 RPC 服务来说，客户端既要发送请求，也需要接收响应）
	go client.receive()
	return client
}

// parseOptions 解析传入 opt，当未传入 opt 或传入 opt 为空，返回默认 opt
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// Dial 根据 network 和 addr 建立连接
func Dial(network, addr string, opts ...*Option) (client *Client, err error) {
	// 解析传入 opt 或是使用默认 opt
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 建立连接
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	// 使用 conn 和 opt 初始化 client
	return NewClient(conn, opt)
}
