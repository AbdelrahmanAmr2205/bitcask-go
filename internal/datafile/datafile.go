package datafile

import (
	"fmt"
	"os"
	"sync"
)

type DataFile struct {
	mu          sync.RWMutex
	id          int   // sequential file identifier
	writeOffset int64 // file offset for the next write operation

	file *os.File
}

func OpenDataFile(path string, fileID int) (*DataFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
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
func (df *DataFile) ReadRecord(offset int64, size uint32) ([]byte, error) {
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
