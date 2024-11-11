package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	r := NewRedis()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go r.handleConn(conn)
	}
}

type Redis struct {
	data map[string]string
}

func NewRedis() *Redis {
	data := make(map[string]string)
	return &Redis{
		data: data,
	}
}

func (r *Redis) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			return
		}

		// fmt.Println("Received data", string(buf[:n]))
		reply := r.parseRESParray(string(buf[:n]))
		conn.Write([]byte(reply))
	}
}

func (r *Redis) parseRESParray(s string) string {
	arr := strings.Split(s, "\r\n")
	var reply string

	switch strings.ToLower(arr[2]) {
	case "echo":
		fmt.Println("entering ECHO")
		strToReply := arr[4]
		if strToReply == "" {
			return ""
		}
		reply = fmt.Sprintf("$%d\r\n%s\r\n", len(strToReply), strToReply)
	case "ping":
		fmt.Println("entering PING")
		reply = "$4\r\nPONG\r\n"
	case "set":
		key := arr[4]
		val := arr[6]
		r.data[key] = val
		reply = "+OK\r\n"
	case "get":
		key := arr[4]
		replyVal := r.data[key]
		reply = fmt.Sprintf("$%d\r\n%s\r\n", len(replyVal), replyVal)
	}
	return reply
}