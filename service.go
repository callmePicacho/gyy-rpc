package gyyrpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

type methodType struct {
	method    reflect.Method // 方法本身
	ArgType   reflect.Type   // 传入参数
	ReplyType reflect.Type   // 返回值
	numCalls  uint64         // 统计方法调用次数
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// newArgv 根据传入参数类型创建对应参数类型的实例
// 原来是指针类型，也新建指针类型；原来是基本数据类型，也新建数据类型
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	// 如果是指针类型，返回其基础类型
	if m.ArgType.Kind() == reflect.Ptr {
		// m.ArgType.Elem() 解引用
		// reflect.New() 返回指定类型的新零值的指针
		argv = reflect.New(m.ArgType.Elem())
	} else {
		// 创建同类型指针，再解引用
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// newReplyv 根据返回参数类型创建对应参数类型的实例
func (m *methodType) newReplyv() reflect.Value {
	// 返回值肯定是个指针类型
	// 得到返回值同类型的零值的指针
	replyv := reflect.New(m.ReplyType.Elem())
	// 如果是 map 或者 slice
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		// 新建一个 map
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		// 新建一个 slice
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

// 代表某个类型作为接收者，全部 RPC 方法的集合
type service struct {
	name   string                 // type 关键字定义的结构体的名称
	typ    reflect.Type           // 结构体类型
	rcvr   reflect.Value          // 结构体的实例本身
	method map[string]*methodType // 存储映射的结构体的所有符合条件的方法
}

// newService
func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	// reflect.Indirect() 如果是指针，进行解引用
	// Type().Name() 返回 reflect.Value 的类型名称，例如 type T struct{}，返回 T
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	// 如果是非导出类型，报错
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// registerMethods
// 过滤出符合条件的方法
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	// 遍历所有方法
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		// 方法的第一个参数为该类型自身，即必须有 2 个入参
		// 方法的返回值只有一个
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		// 方法的返回值类型必须是 error 类型
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		// 第一个入参是传入参数，第二个入参是返回参数
		argType, replyType := mType.In(1), mType.In(2)
		// 必须是可导出类型，或者内置参数
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		// 该方法可以被注册为 RPC 方法
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

func (s *service) call(m *methodType, argv, reply reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	// 拿到函数本身
	f := m.method.Func
	// Call 传入参数调用函数，第一个参数是接收者本身，然后是传入参数，返回参数
	// 得到返回参数列表
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, reply})
	// 只有一个返回参数，且是 error 类型
	// Interface() 将参数类型从 reflect.Value -> interface{}
	// 如果不为 nil，断言为 error 类型返回
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
