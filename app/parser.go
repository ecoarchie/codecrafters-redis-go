package main

import (
	"bufio"
	"io"
	"strconv"
)

type Value struct {
	vType string //type of value
	// str string // store RESP simple string
	// num int // store parsed RESP number
	bulk  string  // store raw RESP bulk string
	array []Value // store RESP array
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
