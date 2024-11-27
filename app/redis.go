package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type RDBconfig struct {
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

type StoredValue struct {
	val     string
	expires time.Time
}

// TODO add the rest of replication options
func (rc *ReplicationConfig) MasterInfo() []byte {
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

func (rc *ReplicationConfig) SlaveInfo() []byte {
	role := fmt.Sprintf("%s:%s", "role", rc.replication.role)
	length := len(role)
	reply := fmt.Sprintf("$%d\r\n%s\r\n", length, role)
	return []byte(reply)

}

type Redis struct {
	commandHandler *CommandHandler
	rdbConf        *RDBconfig
	replConf       *ReplicationConfig
	replicas       []bufio.Writer
}

func NewRedis(rdb *RDBconfig, repl *ReplicationConfig) *Redis {
	return &Redis{
		commandHandler: NewCommandHandler(rdb, repl),
		rdbConf:        rdb,
		replConf:       repl,
	}
}

func (r *Redis) handleConn(conn net.Conn) {
	defer conn.Close()

	addr := strings.Split(conn.RemoteAddr().String(), "]:")
	if addr[1] == r.replConf.replication.master_port {
		fmt.Println("HANDLING request from MASTER PORT")
		fmt.Println("CONN: ", conn)
	}

	client := NewClient(conn)
	for {
		parser := NewParser(client.rw.Reader)
		v, err := parser.Parse()
		if err != nil {
			fmt.Printf("Error is %+v\n", err)
			return
		}
		reply := r.commandHandler.HandleCommand(v)
		if r.replConf.replication.role == "master" {
			if v.vType == "array" && strings.ToUpper(v.array[0].bulk) == "PSYNC" {
				r.replicas = append(r.replicas, *client.rw.Writer)
			}
			if v.vType == "array" && strings.ToUpper(v.array[0].bulk) == "SET" {
				for _, c := range r.replicas {
					c.Write(v.Unmarshal())
					c.Flush()
				}
			}
			client.rw.Write(reply)
			client.rw.Flush()
		}
		if r.replConf.replication.role == "slave" {
			addr := strings.Split(conn.RemoteAddr().String(), "]:")
			if addr[1] != r.replConf.replication.master_port || (addr[1] == r.replConf.replication.master_port && v.array[0].bulk == "REPLCONF" && v.array[1].bulk == "GETACK") {
				client.rw.Write(reply)
				client.rw.Flush()
			}
		}
	}
}

// FIXME return slave connection here to main thread to propagate command from master
func (r *Redis) Handshake() (net.Conn, error) {
	conn, err := r.PingMaster()
	if err != nil {
		return nil, err
	}
	rdbuff := bufio.NewReader(conn)
	ok, err := rdbuff.ReadString('\r')
	if ok != "+PONG\r" {
		fmt.Println("ok is ", ok)
		return nil, fmt.Errorf("didn't receive PONG, received %s instead", ok)
	}
	if err != nil {
		return nil, err
	}

	conn, err = r.ReplConf(conn, rdbuff)
	if err != nil {
		return nil, err
	}

	conn, err = r.Psync(conn, rdbuff)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (r *Redis) PingMaster() (net.Conn, error) {
	addr := fmt.Sprintf("%s:%s", r.replConf.replication.master_host, r.replConf.replication.master_port)
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

func (r *Redis) ReplConf(conn net.Conn, buff *bufio.Reader) (net.Conn, error) {
	buff.Reset(conn)
	_, err := conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%s\r\n", r.replConf.port)))
	if err != nil {
		return nil, err
	}
	ok, err := buff.ReadString('\r')
	if ok != "+OK\r" {
		return nil, fmt.Errorf("error REPLCONF with listening port error, received %s", ok)
	}
	if err != nil {
		return nil, err
	}

	buff.Reset(conn)
	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		return nil, err
	}
	ok, err = buff.ReadString('\r')
	if ok != "+OK\r" {
		return nil, fmt.Errorf("error REPLCONF with capa, received %s", ok)
	}
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (r *Redis) Psync(conn net.Conn, buff *bufio.Reader) (net.Conn, error) {
	buff.Reset(conn)
	_, err := conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if err != nil {
		return nil, err
	}
	buff.ReadBytes('\n') // read full recync message
	lenPart, _ := buff.ReadBytes('\r')
	leng, err := strconv.Atoi(string(lenPart[1 : len(lenPart)-1]))
	if err != nil {
		fmt.Println("Error conververting atoi", err)
	}
	n, err := buff.Discard(leng)
	if err != nil {
		fmt.Printf("error discarding %d bytes - %v\n", n, err)
		return nil, err
	}
	fmt.Printf("Length = %d\nDiscarded bytes - %d\n", leng, n)
	buff.Reset(conn)
	return conn, nil
}

func (r *Redis) ListenPort() net.Listener {
	address := r.replConf.host + ":" + r.replConf.port
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Println("Server is listening on port", r.replConf.port)
	return l
}
