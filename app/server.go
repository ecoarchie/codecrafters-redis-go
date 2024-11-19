package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	config := make(map[string]string)
	dir := flag.String("dir", "", "")
	dbfilename := flag.String("dbfilename", "", "")
	host := flag.String("host", "0.0.0.0", "")
	port := flag.String("port", "6379", "")
	
	flag.Parse()

	config["dir"] = *dir
	config["dbfilename"] = *dbfilename
	config["host"] = *host
	config["port"] = *port

	r := NewRedis(config)
	l := r.ListenPort()
	defer l.Close()

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
	config         map[string]string
}

func (r *Redis) ListenPort() net.Listener {
	address := r.config["host"] +":" + r.config["port"]
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	return l
}

func NewRedis(config map[string]string) *Redis {
	return &Redis{
		commandHandler: *NewCommandHandler(config["dir"], config["dbfilename"]),
		config:         config,
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
