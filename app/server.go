package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	rdbConf := new(RDBconfig)
	replConf := new(ReplicationConfig)

	dir := flag.String("dir", "", "directory for rdb file")
	dbfilename := flag.String("dbfilename", "", "rdb file name")
	host := flag.String("host", "0.0.0.0", "server host addr")
	port := flag.String("port", "6379", "server port")
	replicaof := flag.String("replicaof", "", "command to signal that current server stated as a replica")

	flag.Parse()

	rdbConf.dir = *dir
	rdbConf.dbfilename = *dbfilename
	replConf.host = *host
	replConf.port = *port
	if *replicaof == "" {
		replConf.replication.role = "master"
		//TODO make random alphanumeric string of 40 characters
		replConf.replication.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		replConf.replication.master_repl_offset = 0

	} else {
		replConf.replication.role = "slave"
		addr := strings.Split(*replicaof, " ")
		replConf.replication.master_host = addr[0]
		replConf.replication.master_port = addr[1]
	}

	r := NewRedis(rdbConf, replConf)
	l := r.ListenPort()
	defer l.Close()

	for {
		if r.replConf.replication.role == "slave" {
			masterConn, err := r.Handshake()
			if err != nil {
				fmt.Println("server.go/Handshake(): error from Handshake func", err.Error())
				//TODO try to os.Exit on err
			}
			go r.handleConn(masterConn)
		}
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("server.go/Accept(): error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go r.handleConn(conn)
	}
}
