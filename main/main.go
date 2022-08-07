package main

import (
	"context"
	"gyyrpc"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Foo int

type Args struct {
	Num1, Num2 int
}

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func startServer(addr chan string) {
	var foo Foo
	// 注册 Foo 的方法
	if err := gyyrpc.Register(&foo); err != nil {
		log.Fatal("register error:", err)
	}
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr().String())
	gyyrpc.HandleHTTP()
	addr <- l.Addr().String()
	// 接收客户端请求
	_ = http.Serve(l, nil)
}

func call(addr chan string) {
	// 建立客户端请求
	client, _ := gyyrpc.DialHTTP("tcp", <-addr)
	defer func() {
		_ = client.Close()
	}()

	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{Num1: i, Num2: i * i}
			var reply int
			// 客户端 RPC 调用
			if err := client.Call(context.Background(), "Foo.Sum", args, &reply); err != nil {
				log.Fatal("call Foo.Sum error:", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}

func main() {
	addr := make(chan string)
	go call(addr)
	startServer(addr)
}
