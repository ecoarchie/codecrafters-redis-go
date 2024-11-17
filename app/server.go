package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// file, err := os.Open("dump.rdb")
	// if err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }
	// defer file.Close()

	// scanner := bufio.NewScanner(file)
	// scanner.Split(bufio.ScanBytes)
	// bytes := []byte{}
	// for scanner.Scan() {
	// 	bytes = append(bytes, scanner.Bytes()...)
	// }

	// for _, b := range bytes {
	// 	fmt.Println(b)
	// }

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
	commandHandler CommandHandler
}

func NewRedis() *Redis {
	return &Redis{
		commandHandler: *NewCommandHandler(),
	}
}

func (r *Redis) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		parser := NewParser(conn)
		v, err := parser.Parse()
		if err != nil {
			return
		}
		reply := r.commandHandler.HandleCommand(v)
		conn.Write(reply)
	}
}
