package main

import (
	"fmt"
	"gyyrpc"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

func startServer(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr().String())
	addr <- l.Addr().String()
	// 接收客户端请求
	gyyrpc.Accept(l)
}

func main() {
	// 设置时间打印格式
	log.SetFlags(0)
	addr := make(chan string)
	// 启动服务器
	go startServer(addr)

	// 建立客户端请求
	client, _ := gyyrpc.Dial("tcp", <-addr)
	defer func() {
		_ = client.Close()
	}()

	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := fmt.Sprintf("gyyrpc req %d", i)
			var reply string
			// 客户端 RPC 调用
			if err := client.Call("Foo.sum", args, &reply); err != nil {
				log.Fatal("call Foo.sum error:", err)
			}
			log.Println("reply:", reply)
		}(i)
	}
	wg.Wait()

	rpc.HandleHTTP()
}
