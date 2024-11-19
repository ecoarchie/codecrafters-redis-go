package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

type bit byte

const (
	b7 bit = 0b0000_0001
	b6 bit = 0b0000_0010
	b5 bit = 0b0000_0100
	b4 bit = 0b0000_1000
	b3 bit = 0b0001_0000
	b2 bit = 0b0010_0000
	b1 bit = 0b0100_0000
	b0 bit = 0b1000_0000
)

// TODO rewrite to isZeroBit
func isBitOne(b byte, n bit) bool {
	return b&byte(n) > 0
}

func sizeDecode(r *bufio.Reader) int {
	var buf []byte
	firstByte, _ := r.ReadByte()
	buf = append(buf, firstByte)
	if !isBitOne(firstByte, b0) {
		// case first 2 bits are 0x
		if !isBitOne(firstByte, b1) {
			// case first two bits are 00
			var byteToInt int8
			binary.Read(bytes.NewReader(buf), binary.BigEndian, &byteToInt)
			return int(byteToInt)
		}
		// case first two bits are 01
		secondByte, _ := r.ReadByte()
		buf = append(buf, secondByte)
		var byteToInt int16
		binary.Read(bytes.NewReader(buf), binary.BigEndian, &byteToInt)
		return int(byteToInt)
	}
	//TODO add case where first two bits are 11
	// see 'string encoding' in "Read a key" part of the tutorial

	// case first two bits are 10
	// ignore the first byte, read to buffer the rest 3 bytes
	for i := 0; i < 3; i++ {
		byt, _ := r.ReadByte()
		buf = append(buf, byt)
	}
	var byteToInt uint32
	binary.Read(bytes.NewReader(buf[1:]), binary.BigEndian, &byteToInt)
	return int(byteToInt)
}

type RDBconn struct {
	dir        string
	dbfilename string
}

func NewRDBconn(dir, filename string) *RDBconn {
	if dir == "" && filename == "" {
		return nil
	}
	return &RDBconn{
		dir:        dir,
		dbfilename: filename,
		// path: fmt.Sprintf("%s/%s", dir, filename),
	}
}

func (rdb *RDBconn) openRDBfile() (*bufio.Reader, error) {
	rdbFile, err := os.Open(rdb.dir + "/" + rdb.dbfilename)
	if err != nil {
		// treat db as empty
		//TODO create 'no file error' type
		return nil, err
	}
	defer rdbFile.Close()

	reader := bufio.NewReader(rdbFile)
	err = healthCheck(reader)
	if err != nil {
		//TODO handle error
		fmt.Println("Failed healthcheck", err)
		// return nil, err
		os.Exit(1)
	}
	return reader, nil
}

func (rdb *RDBconn) GetKeysWithPattern(pattern string) (keys [][]byte, err error) {
	reader, err := rdb.openRDBfile()
	if err != nil {
		// treat db as empty
		return nil, nil
	}
	// read till FB - indicates a resizedb field, which follows by 2 bytes with db size info
	reader.ReadBytes('\xFB')
	if pattern == "*" {
		keys = getAllKeysFrom(reader)
	}
	//TODO add regex patterns
	return keys, nil
}

func getAllKeysFrom(r *bufio.Reader) [][]byte {
	hashTableSize := sizeDecode(r) // size of the corresponding hash table
	r.Discard(1)                   // skip size of the corresponding expire hash table
	keys := make([][]byte, hashTableSize)
	for i := 0; i < hashTableSize; i++ {
		r.ReadByte() // skip type of value encoding byte
		keyLength := sizeDecode(r)
		keys[i] = make([]byte, keyLength)
		r.Read(keys[i])
		skipValue(r)
	}
	return keys
}

// advance reader skipping value bytes
func skipValue(r *bufio.Reader) {
	valLength := sizeDecode(r)
	for i := 0; i < valLength; i++ {
		r.ReadByte()
	}
}

func healthCheck(r *bufio.Reader) error {
	magicString, err := r.Peek(5)
	if err != nil {
		return err
	}
	if string(magicString) != "REDIS" {
		return fmt.Errorf("error not a rdb file")
	}
	return nil
}

func (rdb *RDBconn) LoadFromRDStoMemory() (map[string]StoredValue, error) {
	store := make(map[string]StoredValue)
	reader, err := rdb.openRDBfile()
	if err != nil {
		return nil, err
	}
	// read till FB - indicates a resizedb field, which follows by 2 bytes with db size info
	reader.ReadBytes('\xFB')
	reader.Discard(2) // skip size of the corresponding hash table and expire hash table
	for {
		byt, err := reader.ReadByte()
		if err != nil || byt == '\xFF' {
			break
		}
		if byt == '\xFE' {
			continue
		}
		var expires time.Time
		if byt == '\xFD' || byt == '\xFC' {
			expires = getExpireDate(byt, reader)
		}
		keyLength := sizeDecode(reader)
		keyBuf := make([]byte, keyLength)
		reader.Read(keyBuf)

		valLength := sizeDecode(reader)
		valBuf := make([]byte, valLength)
		reader.Read(valBuf)
		val := StoredValue{
			val:     string(valBuf),
			expires: expires,
		}
		store[string(keyBuf)] = val
	}
	return store, nil
}

func getExpireDate(b byte, r *bufio.Reader) time.Time {
	var expires time.Time
	// expiry time in seconds followed by 4 byte unsigned int
	if b == '\xFD' {
		var expiresInt32 uint32
		expireBytes := make([]byte, 4)
		r.Read(expireBytes)
		binary.Read(bytes.NewReader(expireBytes), binary.LittleEndian, &expiresInt32)
		expires = time.Unix(int64(expiresInt32), 0)
	}
	// expiry time in milliseconds followed by 8 byte unsigned long
	if b == '\xFC' {
		var expiresInt64 uint64
		expireBytes := make([]byte, 8)
		r.Read(expireBytes)
		binary.Read(bytes.NewReader(expireBytes), binary.LittleEndian, &expiresInt64)
		expires = time.UnixMilli(int64(expiresInt64))
	}
	//TODO add encoding type prcessing
	r.ReadByte() // skip encoding byte
	return expires
}
