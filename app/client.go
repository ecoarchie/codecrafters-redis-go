package main

import (
	"bufio"
	"net"
)

type Client struct {
	rw *bufio.ReadWriter
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		rw: bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
	}
}