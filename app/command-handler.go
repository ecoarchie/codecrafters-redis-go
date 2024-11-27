package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	emptyRDBhash = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

type CommandHandler struct {
	data     map[string]StoredValue
	rdbconn  *RDBconn
	replConf *ReplicationConfig
	mu       sync.RWMutex
}

func NewCommandHandler(rdb *RDBconfig, repl *ReplicationConfig) *CommandHandler {
	rdbConn := NewRDBconn(rdb.dir, rdb.dbfilename)

	data := make(map[string]StoredValue)
	if rdbConn != nil {
		data, _ = rdbConn.LoadFromRDStoMemory()
	}
	return &CommandHandler{
		data:     data,
		rdbconn:  rdbConn,
		replConf: repl,
	}
}

func (ch *CommandHandler) HandleCommand(v Value) []byte {
	var repl Value
	if v.vType == "array" {
		command := strings.ToLower(v.array[0].bulk)
		fmt.Println("HANDLE COMMAND: ", command)
		switch command {
		case "ping":
			return ch.ping(v)
		case "echo":
			return ch.echo(v)
		case "set":
			return ch.set(v)
		case "get":
			return ch.get(v)
		case "config":
			return ch.config(v)
		case "keys":
			return ch.keys(v)
		case "info":
			return ch.info(v)
		case "replconf":
			return ch.replconf(v)
		case "psync":
			return ch.psync(v)
		}
	} else {
		return []byte("$5\r\nERROR\r\n")
	}
	return repl.OK()
}

type setOptions struct {
	PX int
}

func (ch *CommandHandler) ping(_ Value) []byte {
	repl := Value{
		vType: "str",
		str:   "PONG",
	}
	return repl.Unmarshal()
}

func (ch *CommandHandler) echo(v Value) []byte {
	repl := Value{
		vType: "bulk",
		bulk:  v.array[1].bulk,
	}
	return repl.Unmarshal()
}

func (ch *CommandHandler) set(v Value) []byte {
	var repl Value
	key := v.array[1].bulk
	value := v.array[2].bulk
	var opts setOptions
	if len(v.array) > 3 {
		opts = ch.parseSetOpts(v.array[2:])
	}
	ch.setValue(key, value, opts)
	return repl.OK()
}

func (ch *CommandHandler) get(v Value) []byte {
	key := v.array[1].bulk
	val := ch.getValue(key)
	var repl Value
	repl.vType = "bulk"
	if val == "" {
		repl.bulk = ""
		return repl.Unmarshal()
	}
	repl.bulk = val
	return repl.Unmarshal()
}

func (ch *CommandHandler) config(v Value) []byte {
	var repl Value
	repl.vType = "array"
	arg := strings.ToLower(v.array[1].bulk)
	if arg == "get" {
		key := v.array[2].bulk
		repl.array = append(repl.array, Value{vType: "bulk", bulk: key})
		if key == "dir" {
			repl.array = append(repl.array, Value{vType: "bulk", bulk: ch.rdbconn.dir})
		}
		if key == "dbfilename" {
			repl.array = append(repl.array, Value{vType: "bulk", bulk: ch.rdbconn.dbfilename})
		}
		return repl.Unmarshal()
	}
	return nil
}

func (ch *CommandHandler) keys(v Value) []byte {
	var repl Value
	repl.vType = "array"

	pattern := v.array[1].bulk
	keys, err := ch.rdbconn.GetKeysWithPattern(pattern)
	if err != nil {
		fmt.Println("error getting keys", err)
		return nil
	}
	if len(keys) == 0 {
		return repl.Unmarshal()
	}
	for _, k := range keys {
		var val Value
		val.vType = "bulk"
		val.bulk = string(k)
		repl.array = append(repl.array, val)
	}
	return repl.Unmarshal()
}

func (ch *CommandHandler) info(v Value) []byte {
	arg := strings.ToLower(v.array[1].bulk)
	if arg == "replication" && ch.replConf.replication.role == "master" {
		return ch.replConf.MasterInfo()
	}

	return ch.replConf.SlaveInfo()
}

func (ch *CommandHandler) replconf(v Value) []byte {
	var repl Value

	arg := strings.ToUpper(v.array[1].bulk)
	argVal := strings.ToLower(v.array[2].bulk)
	fmt.Println(arg, "\n", argVal)
	if arg == "GETACK" && argVal == "*" {
		repl.vType = "array"
		repl.array = append(repl.array, Value{vType: "bulk", bulk: "REPLCONF"}, Value{vType: "bulk", bulk: "ACK"}, Value{vType: "bulk", bulk: "0"})
		return repl.Unmarshal()
	}
	return repl.OK()
}

func (ch *CommandHandler) psync(_ Value) []byte {
	//TODO add check for args ? and 0 for full resync
	reply := fmt.Sprintf("+FULLRESYNC %s %d\r\n", ch.replConf.replication.master_replid, ch.replConf.replication.master_repl_offset)

	decoded, _ := hex.DecodeString(emptyRDBhash)

	var res []byte
	res = append(res, []byte(reply)...)
	res = append(res, []byte(fmt.Sprintf("$%d\r\n", len(decoded)))...)
	res = append(res, decoded...) //decoded RDB hash is not a bulk string so without CRLF
	return res
}

func (ch *CommandHandler) setValue(key, val string, opts setOptions) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	newVal := StoredValue{}
	newVal.val = val
	var now time.Time
	if opts.PX != 0 {
		now = time.Now()
		newVal.expires = now.Add(time.Millisecond * time.Duration(opts.PX))
	}
	//TODO add support of other options
	ch.data[key] = newVal
}

func (ch *CommandHandler) parseSetOpts(a []Value) setOptions {
	opts := setOptions{}
	for i, v := range a {
		if strings.ToLower(v.bulk) == "px" {
			pxVal, _ := strconv.Atoi(a[i+1].bulk) //TODO handle error
			opts.PX = pxVal
			break
		}
	}
	return opts
}

func (ch *CommandHandler) getValue(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	v, isKey := ch.data[key]
	if !isKey {
		fmt.Println("NO KEY")
		return ""
	}

	if !v.expires.IsZero() {
		isLost := v.expires.Before(time.Now())
		if isLost {
			return ""
		}
	}
	return v.val
}
