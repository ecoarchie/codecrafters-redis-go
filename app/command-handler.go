package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StoredValue struct {
	val       string
	validTill time.Time
}

type CommandHandler struct {
	data map[string]StoredValue
	mu   sync.RWMutex
}

func NewCommandHandler() *CommandHandler {
	data := make(map[string]StoredValue)
	return &CommandHandler{
		data: data,
	}
}

type setOptions struct {
	PX int
}

func (ch *CommandHandler) HandleCommand(v Value) []byte {
	if v.vType == "array" {
		command := strings.ToLower(v.array[0].bulk)
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
		newVal.validTill = now.Add(time.Millisecond * time.Duration(opts.PX))
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

	if !v.validTill.IsZero() {
		lost := v.validTill.Before(time.Now())
		if lost {
			return ""
		}
	}
	return v.val
}
