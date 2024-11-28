package main

import (
	"bufio"
)

type Client struct {
	rw *bufio.ReadWriter
}

func NewClient(br *bufio.Reader, bw *bufio.Writer) *Client {
	return &Client{
		rw: bufio.NewReadWriter(bufio.NewReader(br), bufio.NewWriter(bw)),
	}
}
