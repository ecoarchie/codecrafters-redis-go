package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Value struct {
	vType string //type of value
	str string // store RESP simple string
	num int // store parsed RESP number
	bulk  string  // store raw RESP bulk string
	array []Value // store RESP array
}

func (v *Value) OK() []byte {
		v.vType = "str"
		v.str = "OK"
		return v.Unmarshal()
}

func (v *Value) Unmarshal() []byte {
	switch v.vType {
	case "str":
		return v.toStr()
	case "num":
		return v.toNum()
	case "bulk":
		return v.toBulk()
	case "array":
		return v.toArray()
	}
	return nil
}

func (v *Value) toStr() []byte {
	reply := fmt.Sprintf("+%s\r\n", v.str)
	return []byte(reply)
}

func (v *Value) toNum() []byte {
	reply := fmt.Sprintf(":%d\r\n", v.num)
	return []byte(reply)
}

func (v *Value) toBulk() []byte {
	var reply string
	if v.bulk == "" {
		reply = "$-1\r\n"
	} else {
		reply = fmt.Sprintf("$%d\r\n%s\r\n", len(v.bulk), v.bulk)
	}
	return []byte(reply)
}

func (v *Value) toArray() []byte {
	if len(v.array) == 0 {
		return []byte("*0\r\n")
	}
	begin := fmt.Sprintf("*%d\r\n", len(v.array))
	var vals []string
	for _, v := range v.array {
		length := fmt.Sprintf("$%d\r\n", len(v.bulk))
		vals = append(vals, fmt.Sprintf("%s%s", length, v.bulk))
	}
	valsStr := strings.Join(vals, "\r\n")
	reply := fmt.Sprintf("%s%s\r\n", begin, valsStr)
	return []byte(reply)
}


const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Parser struct {
	reader *bufio.Reader
}

func NewParser(rd io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(rd),
	}
}

func (p *Parser) readLine() (line []byte, n int, err error) {
	for {
		b, err := p.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (p *Parser) readInteger() (x int, n int, err error) {
	xStr, n, err := p.readLine()
	if err != nil {
		return 0, n, err
	}
	i64, err := strconv.ParseInt(string(xStr), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (p *Parser) Parse() (Value, error) {
	vType, err := p.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch vType {
	case ARRAY:
		return p.readArray()
	case BULK:
		return p.readBulk()
	default:
		return Value{}, nil
	}
}

func (p *Parser) readArray() (Value, error) {
	v := Value{}
	v.vType = "array"

	length, _, err := p.readInteger()
	if err != nil {
		return v, err
	}
	v.array = make([]Value, length)

	for i := 0; i < length; i++ {
		val, err := p.Parse()
		if err != nil {
			return v, err
		}
		v.array[i] = val
	}
	return v, nil
}

func (p *Parser) readBulk() (Value, error) {
	v := Value{}
	v.vType = "bulk"

	length, _, err := p.readInteger()
	if err != nil {
		return v, err
	}
	bulk := make([]byte, length)
	p.reader.Read(bulk)
	v.bulk = string(bulk)

	// read out trailing CRLF
	p.readLine()

	return v, nil
}
