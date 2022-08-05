package codec

import "io"

// Header 请求头
type Header struct {
	ServiceMethod string // 服务名和方法名，格式为："服务名.方法名"
	Seq           uint64 // 请求的序号，用来区分不同的请求
	Error         string // 错误信息
}

// Codec 编解码接口
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

// 工厂模式，在 NewCodecFuncMap 中根据 Type 返回相应的构造函数
func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
