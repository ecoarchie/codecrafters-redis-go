package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	redisConfig := new(RedisConfig)

	// config := make(map[string]string)
	dir := flag.String("dir", "", "")
	dbfilename := flag.String("dbfilename", "", "")
	host := flag.String("host", "0.0.0.0", "")
	port := flag.String("port", "6379", "")

	replicaof := flag.String("replicaof", "", "")

	flag.Parse()

	fmt.Println("replicaof flag = ", *replicaof == "")

	redisConfig.rds.dir = *dir
	redisConfig.rds.dbfilename = *dbfilename
	redisConfig.replication.host = *host
	redisConfig.replication.port = *port
	if *replicaof == "" {
		redisConfig.replication.replication.role = "master"
	} else {
		redisConfig.replication.replication.role = "slave"
	}

	r := NewRedis(redisConfig)
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

type RDSconfig struct {
	dir        string
	dbfilename string
}

type ReplicationConfig struct {
	host        string
	port        string
	replication struct {
		role string
	}
}

func (rc *ReplicationConfig) ByteString() []byte {
	roleStr := []byte(fmt.Sprintf("$%d\r\n%s:%s\r\n", len("role:")+len(rc.replication.role), "role", rc.replication.role))
	return roleStr
}

type RedisConfig struct {
	rds         RDSconfig
	replication ReplicationConfig
}

type Redis struct {
	commandHandler CommandHandler
	config         *RedisConfig
	// config         map[string]string
}

func NewRedis(config *RedisConfig) *Redis {
	return &Redis{
		commandHandler: *NewCommandHandler(config),
		config:         config,
	}
}

func (r *Redis) ListenPort() net.Listener {
	address := r.config.replication.host + ":" + r.config.replication.port
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Println("Server is listening on port", r.config.replication.port)
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
