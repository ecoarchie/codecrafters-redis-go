package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StoredValue struct {
	val     string
	expires time.Time
}

type CommandHandler struct {
	data    map[string]StoredValue
	rdbconn *RDBconn
	config  *RedisConfig
	mu      sync.RWMutex
}

func NewCommandHandler(config *RedisConfig) *CommandHandler {
	rdbConn := NewRDBconn(config.rds.dir, config.rds.dbfilename)

	data := make(map[string]StoredValue)
	if rdbConn != nil {
		data, _ = rdbConn.LoadFromRDStoMemory()
	}
	return &CommandHandler{
		data:    data,
		rdbconn: rdbConn,
		config:  config,
	}
}

type setOptions struct {
	PX int
}

// FIXME rewrite this ugly function
func (ch *CommandHandler) HandleCommand(v Value) []byte {
	if v.vType == "array" {
		command := strings.ToLower(v.array[0].bulk)
		fmt.Println(command)
		switch command {
		case "ping":
			return []byte("+PONG\r\n")
		case "echo":
			return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v.array[1].bulk), v.array[1].bulk))
		case "set":
			key := v.array[1].bulk
			value := v.array[2].bulk
			var opts setOptions
			if len(v.array) > 3 {
				opts = ch.parseSetOpts(v.array[2:])
			}
			ch.setValue(key, value, opts)
		case "get":
			key := v.array[1].bulk
			val := ch.getValue(key)
			if val == "" {
				return []byte("$-1\r\n")
			}
			return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
		case "config":
			arg := strings.ToLower(v.array[1].bulk)
			if arg == "get" {
				key := v.array[2].bulk
				var val string
				if key == "dir" {
					val = ch.rdbconn.dir
				}
				if key == "dbfilename" {
					val = ch.rdbconn.dbfilename
				}
				return []byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val))
			}
		case "keys":
			pattern := v.array[1].bulk
			keys, err := ch.rdbconn.GetKeysWithPattern(pattern)
			if err != nil {
				fmt.Println("error getting keys", err)
				return nil
			}
			fmt.Printf("%x\n", keys)
			if len(keys) == 0 {
				return []byte("*0\r\n")
			}
			reply := []byte(fmt.Sprintf("*%d\r\n", len(keys)))
			for i := 0; i < len(keys); i++ {
				reply = append(reply, []byte(fmt.Sprintf("$%d\r\n", len(keys[i])))...)
				reply = append(reply, keys[i]...)
				reply = append(reply, []byte("\r\n")...)
			}
			return reply
		case "info":
			arg := strings.ToLower(v.array[1].bulk)
			if arg == "replication" {
				return ch.config.replConf.ByteString()
			}
		case "replconf":
			return []byte("+OK\r\n")
		case "psync":
			reply := fmt.Sprintf("+FULLRESYNC %s %d\r\n", ch.config.replConf.replication.master_replid, ch.config.replConf.replication.master_repl_offset)
			emptyRDB := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

			decoded, _ := hex.DecodeString(emptyRDB)

			var res []byte
			res = append(res, []byte(reply)...)
			res = append(res, []byte(fmt.Sprintf("$%d\r\n", len(decoded)))...)
			res = append(res, decoded...)
			return res
		}
	} else {
		return []byte("$5\r\nERROR\r\n")
	}
	return []byte("+OK\r\n")
}

func Hex2Bin(src string) (string, error) {
	ui, err := strconv.ParseUint(src, 16, 64)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%0176b", ui), nil
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
