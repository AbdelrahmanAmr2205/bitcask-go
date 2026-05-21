package datafile

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const HeaderSize = 12

type Header struct {
	timepstamp uint32 // UNIX epoch timestamp
	keySize    uint32
	valSize    uint32
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
			timepstamp: timestamp,
			keySize:    uint32(len(key)),
			valSize:    uint32(len(val)),
		},
		Key: key,
		Val: val,
	}
}

func (r *Record) Size() uint64 {
	return uint64(HeaderSize + r.Header.keySize + r.Header.valSize)
}

func (r *Record) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, r.Size()))

	if err := binary.Write(buf, binary.LittleEndian, &r.Header); err != nil {
		return nil, fmt.Errorf("failed to encode record header: %w", err)
	}

	buf.WriteString(r.Key)
	buf.Write(r.Val)

	return buf.Bytes(), nil
}
