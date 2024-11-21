package main

import (
	"bufio"
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
		master_host        string
		master_port        string
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

func (r *Redis) Handshake() error {
	conn, err := r.PingMaster()
	if err != nil {
		return err
	}
	rdbuff := bufio.NewReader(conn)
	ok, err := rdbuff.ReadString('\r')
	if ok != "+PONG\r" {
		fmt.Println("ok is ", ok)
		return fmt.Errorf("didn't receive PONG, received %s instead", ok)
	}
	if err != nil {
		return err
	}

	err = r.ReplConf(conn, rdbuff)
	if err != nil {
		return err
	}

	err = r.Psync(conn, rdbuff)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

func (r *Redis) PingMaster() (net.Conn, error) {
	addr := fmt.Sprintf("%s:%s", r.config.replConf.replication.master_host, r.config.replConf.replication.master_port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	if _, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		fmt.Println(err)
		return nil, err
	}
	return conn, nil
}

func (r *Redis) ReplConf(conn net.Conn, buff *bufio.Reader) error {
	buff.Reset(conn)
	_, err := conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%s\r\n", r.config.replConf.port)))
	if err != nil {
		return err
	}
	ok, err := buff.ReadString('\r')
	// ok, err := bufio.NewReader(conn).ReadString('\r')
	if ok != "+OK\r" {
		return fmt.Errorf("error REPLCONF with listening port error, received %s", ok)
	}
	if err != nil {
		return err
	}

	buff.Reset(conn)
	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		return err
	}
	ok, err = buff.ReadString('\r')
	// ok, err = bufio.NewReader(conn).ReadString('\r')
	if ok != "+OK\r" {
		return fmt.Errorf("error REPLCONF with capa, received %s", ok)
	}
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) Psync(conn net.Conn, buff *bufio.Reader) error {
	buff.Reset(conn)
	_, err := conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if err != nil {
		return err
	}
	// ok, err := bufio.NewReader(conn).ReadString('\r')
	// if ok != "+OK\r" {
	// 	return fmt.Errorf("wrong reply")
	// }
	// if err != nil {
	// 	return err
	// }
	return nil
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
