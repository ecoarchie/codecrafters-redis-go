package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

type RDSconfig struct {
	dir        string
	dbfilename string
}

type ReplicationConfig struct {
	host        string
	port        string
	replication struct {
		role               string
		master_replid      string
		master_repl_offset int
	}
}

// TODO add the rest of replication options
func (rc *ReplicationConfig) ByteString() []byte {
	role := fmt.Sprintf("%s:%s", "role", rc.replication.role)
	masterReplID := fmt.Sprintf("%s:%s", "master_replid", rc.replication.master_replid)
	masterReplOffset := fmt.Sprintf("%s:%d", "master_repl_offset", rc.replication.master_repl_offset)

	var arr []string
	arr = append(arr, role)
	arr = append(arr, masterReplID)
	arr = append(arr, masterReplOffset)
	resStr := strings.Join(arr, "\r\n")
	
	len := len(resStr)
	reply := fmt.Sprintf("$%d\r\n%s\r\n", len, resStr)
	return []byte(reply)
}

type RedisConfig struct {
	rds      RDSconfig
	replConf ReplicationConfig
}

type Redis struct {
	commandHandler CommandHandler
	config         *RedisConfig
}

func NewRedis(config *RedisConfig) *Redis {
	return &Redis{
		commandHandler: *NewCommandHandler(config),
		config:         config,
	}
}

func (r *Redis) ListenPort() net.Listener {
	address := r.config.replConf.host + ":" + r.config.replConf.port
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Println("Server is listening on port", r.config.replConf.port)
	return l
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
