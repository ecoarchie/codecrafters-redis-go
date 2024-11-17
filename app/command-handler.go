package main

import (
	"flag"
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
	config  map[string]string
	rdbconn *RDBconn
	mu      sync.RWMutex
}

func NewCommandHandler() *CommandHandler {
	dir := flag.String("dir", "", "")
	dbfilename := flag.String("dbfilename", "", "")
	flag.Parse()

	data := make(map[string]StoredValue)
	config := make(map[string]string)
	config["dir"] = *dir
	config["dbfilename"] = *dbfilename
	return &CommandHandler{
		data:    data,
		config:  config,
		rdbconn: NewRDBconn(*dir, *dbfilename),
	}
}

type setOptions struct {
	PX int
}

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
				val, present := ch.config[key]
				if !present {
					return []byte("$-1\r\n")
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
		}
	} else {
		return []byte("$5\r\nERROR\r\n")
	}
	return []byte("+OK\r\n")
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
