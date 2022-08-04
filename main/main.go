package main

import (
	"encoding/json"
	"fmt"
	gyyrpc "gyyRPC"
	"gyyRPC/codec"
	"log"
	"net"
	"time"
)

func startServer(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr().String())
	addr <- l.Addr().String()
	gyyrpc.Accept(l)
}

func main() {
	// 设置时间打印格式
	log.SetFlags(0)
	addr := make(chan string)
	// 启动服务器
	go startServer(addr)

	conn, _ := net.Dial("tcp", <-addr)
	defer func() {
		conn.Close()
	}()

	time.Sleep(time.Second)
	_ = json.NewEncoder(conn).Encode(gyyrpc.DefaultOption)
	cc := codec.NewGobCodec(conn)
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "foo.sum",
			Seq:           uint64(i),
		}
		cc.Write(h, fmt.Sprintf("gyyrpc req %d", h.Seq))
		cc.ReadHeader(h)
		var reply string
		cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
