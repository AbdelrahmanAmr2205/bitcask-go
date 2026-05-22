package datafile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const HintHeaderSize = 20

type HintHeader struct {
	Timestamp uint32
	KeySize   uint32
	ValSize   uint32
	ValOffset int64
}

type HintRecord struct {
	Header HintHeader
	Key    string
}

func NewHintRecord(timestamp uint32, keySize uint32, valSize uint32, valOffset int64, key string) HintRecord {
	return HintRecord{
		Header: HintHeader{
			Timestamp: timestamp,
			KeySize:   keySize,
			ValSize:   valSize,
			ValOffset: valOffset,
		},
		Key: key,
	}
}

func (h *HintRecord) Size() uint64 {
	return uint64(HintHeaderSize + h.Header.KeySize)
}

func (h *HintRecord) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, h.Size()))

	if err := binary.Write(buf, binary.LittleEndian, &h.Header); err != nil {
		return nil, fmt.Errorf("failed to encode hint header: %w", err)
	}

	buf.WriteString(h.Key)
	return buf.Bytes(), nil
}

func DecodeHintHeader(r io.Reader) (HintHeader, error) {
	var h HintHeader
	if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
		return HintHeader{}, err
	}
	return h, nil
}

func ScanHintFile(path string) ([]ScannedEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []ScannedEntry

	for {
		header, err := DecodeHintHeader(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("corrupted hint header read: %w", err)
		}

		keyBuf := make([]byte, header.KeySize)
		if _, err := io.ReadFull(file, keyBuf); err != nil {
			return nil, fmt.Errorf("failed to read key during hint recovery: %w", err)
		}
		key := string(keyBuf)

		entries = append(entries, ScannedEntry{
			Key:       key,
			ValSize:   header.ValSize,
			ValOffset: header.ValOffset,
			Timestamp: header.Timestamp,
		})
	}

	return entries, nil
}
