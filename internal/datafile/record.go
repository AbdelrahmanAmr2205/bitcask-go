package datafile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const HeaderSize = 12

type Header struct {
	Timestamp uint32 // UNIX epoch timestamp
	KeySize   uint32
	ValSize   uint32
}

// Record represents the complete structural data layout exactly as it exists on disk.
type Record struct {
	Header Header
	Key    string
	Val    []byte
}

func NewRecord(key string, val []byte, timestamp uint32) Record {
	return Record{
		Header: Header{
			Timestamp: timestamp,
			KeySize:   uint32(len(key)),
			ValSize:   uint32(len(val)),
		},
		Key: key,
		Val: val,
	}
}

func (r *Record) Size() uint64 {
	return uint64(HeaderSize + r.Header.KeySize + r.Header.ValSize)
}

func (r *Record) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, r.Size()))

	if err := binary.Write(buf, binary.LittleEndian, &r.Header); err != nil {
		return nil, fmt.Errorf("failed to encode record header: %w", err)
	}

	buf.WriteString(r.Key)
	buf.Write(r.Val)

	return buf.Bytes(), nil
}

func DecodeHeader(r io.Reader) (Header, error) {
	var h Header
	if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
		return Header{}, err
	}
	return h, nil
}
