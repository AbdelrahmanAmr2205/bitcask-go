package datafile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type DataFile struct {
	mu          sync.RWMutex
	id          int   // sequential file identifier
	writeOffset int64 // file offset for the next write operation

	file *os.File
}

func OpenDataFile(path string, fileID int) (*DataFile, error) {
	fileName := fmt.Sprintf("%010d.data", fileID)
	file, err := os.OpenFile(filepath.Join(path, fileName), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to read database file stats: %w", err)
	}

	return &DataFile{
		mu:          sync.RWMutex{},
		id:          fileID,
		writeOffset: fileInfo.Size(),
		file:        file,
	}, nil
}

// takes an encoded record and returns the offset to its beginning
func (df *DataFile) WriteRecord(data []byte) (int64, error) {
	df.mu.Lock()
	defer df.mu.Unlock()

	startingOffset := df.writeOffset
	_, err := df.file.Write(data)
	if err != nil {
		return -1, fmt.Errorf("failed appending payload bytes to disk log: %w", err)
	}

	df.writeOffset += int64(len(data))

	return startingOffset, nil
}

// takes the the offset and size of the value, not the whole record, and returns only the value as well
func (df *DataFile) ReadValue(offset int64, size uint32) ([]byte, error) {
	val := make([]byte, size)
	_, err := df.file.ReadAt(val, offset)
	if err != nil {
		return nil, fmt.Errorf("failed reading value: %w", err)
	}

	return val, nil
}

// Close gracefully terminates open file handlers safely.
func (df *DataFile) Close() error {
	df.mu.Lock()
	defer df.mu.Unlock()
	return df.file.Close()
}

func (df *DataFile) ID() int {
	return df.id
}

func (df *DataFile) Size() int64 {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.writeOffset
}

type ScannedEntry struct {
	Key       string
	ValSize   uint32
	ValOffset int64
	Timestamp uint32
}

func (df *DataFile) ScanRecords() ([]ScannedEntry, error) {
	// Temporarily seek to the absolute beginning of the file descriptor for sequential streaming
	if _, err := df.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []ScannedEntry
	var currentOffset int64 = 0

	for {
		// 1. Decode the fixed header block
		header, err := DecodeHeader(df.file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // Clean end of file segment reached
			}
			return nil, fmt.Errorf("corrupted recovery header read: %w", err)
		}

		// 2. Read variable-length key bytes sequentially
		keyBuf := make([]byte, header.KeySize)
		if _, err := io.ReadFull(df.file, keyBuf); err != nil {
			return nil, fmt.Errorf("failed to read key during recovery: %w", err)
		}
		key := string(keyBuf)

		// 3. Skip past the Value payload entirely to optimize boot memory performance.
		// Standard file streaming skips over bytes efficiently without allocating them to memory.
		if _, err := df.file.Seek(int64(header.ValSize), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("failed to seek past payload during recovery: %w", err)
		}

		// calculate tracking layout specs
		recordSize := uint32(HeaderSize + header.KeySize + header.ValSize)
		valOffset := currentOffset + int64(HeaderSize) + int64(header.KeySize)

		entries = append(entries, ScannedEntry{
			Key:       key,
			ValSize:   header.ValSize,
			ValOffset: valOffset,
			Timestamp: header.Timestamp,
		})

		// Track offset position to correctly map our pointers
		currentOffset += int64(recordSize)
	}

	return entries, nil
}
