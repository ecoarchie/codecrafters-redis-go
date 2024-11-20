package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
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

	redisConfig.rds.dir = *dir
	redisConfig.rds.dbfilename = *dbfilename
	redisConfig.replConf.host = *host
	redisConfig.replConf.port = *port
	if *replicaof == "" {
		redisConfig.replConf.replication.role = "master"
		//TODO make random alphanumeric string of 40 characters
		redisConfig.replConf.replication.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		redisConfig.replConf.replication.master_repl_offset = 0

	} else {
		redisConfig.replConf.replication.role = "slave"
		addr := strings.Split(*replicaof, " ")
		redisConfig.replConf.replication.master_host = addr[0]
		redisConfig.replConf.replication.master_port = addr[1]
	}

	r := NewRedis(redisConfig)
	if r.config.replConf.replication.role == "slave" {
		err := r.Handshake()
		if err != nil {
			fmt.Println(err)
		}
	}
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
