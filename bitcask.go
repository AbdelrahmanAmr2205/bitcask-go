package bitcask

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/AbdelrahmanAmr2205/bitcask-go/internal/datafile"
)

type DB struct {
	muFiles sync.RWMutex // read-write mutex for thread-safe access of the files map

	muKeyDir sync.RWMutex // read-write mutex for concurrent keyDir access
	keyDir   map[string]keyDirEntry

	writeChan chan writeRequest

	activeFile *datafile.DataFile
	files      map[int]*datafile.DataFile
	config     Config
	cancelFunc context.CancelFunc // Coordinates clean, non-blocking closures
}

type writeRequest struct {
	key       string
	value     []byte
	timestamp uint32
	respCh    chan writeResponse
}

type writeResponse struct {
	err error
}

type keyDirEntry struct {
	valsize   uint32
	valOffset int64
	fileID    int
	timestamp uint32
}

type Config struct {
	Directory                string        // Path to the data directory on disk
	MaxActiveFileSize        int64         // Max size in bytes before rotating the active file
	CompactInterval          time.Duration // Time window between background compaction merges
	SyncPeriod               time.Duration // Optional background interval to force fsync (0 means disabled)
	ExpectedWriteRate        int64         // expected write requests size per second (eg. 1024 bytes/sec)
	ExpectedFilesPerInterval int           // expected number of generated files per compaction interval
}

func (db *DB) startWriteLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			//TODO: add draining logic before terminating
			db.cancelFunc()
			return
		case req := <-db.writeChan:
			record := datafile.NewRecord(req.key, req.value, req.timestamp)
			encoded, err := record.Encode()
			if err != nil {
				req.respCh <- writeResponse{err: err}
				continue
			}

			offset, err := db.activeFile.WriteRecord(encoded)
			if err != nil {
				req.respCh <- writeResponse{err: err}
				continue
			}

			db.writeKeyDirEntry(req.key, keyDirEntry{
				valsize:   uint32(len(record.Val)),
				valOffset: offset + datafile.HeaderSize + int64(len(req.key)),
				timestamp: req.timestamp,
				fileID:    db.activeFile.ID(),
			})

			req.respCh <- writeResponse{err: nil}

			if db.activeFile.Size() >= db.config.MaxActiveFileSize {
				db.createNewActiveFile(db.activeFile.ID() + 1)
			}
		}
	}
}

func (db *DB) createNewActiveFile(fileID int) error {
	var err error
	for range 5 {
		newFile, err := datafile.OpenDataFile(db.config.Directory, fileID)
		if err == nil {
			db.muFiles.Lock()
			db.activeFile = newFile
			db.files[fileID] = newFile
			db.muFiles.Unlock()
			return nil
		}
	}

	return fmt.Errorf("couldn't create new active file: %w", err)
}

func (db *DB) Put(key string, val []byte) error {
	respCh := make(chan writeResponse, 1)
	req := writeRequest{
		key:       key,
		value:     val,
		timestamp: uint32(time.Now().Unix()),
		respCh:    respCh,
	}

	db.writeChan <- req

	res := <-respCh
	return res.err
}

func (db *DB) Get(key string) ([]byte, error) {
	entry, exists := db.readKeyDirEntry(key)
	if !exists {
		return nil, errors.New("Key does not exist")
	}

	db.muFiles.RLock()
	targetFile, fileExists := db.files[entry.fileID]
	db.muFiles.RUnlock()

	if !fileExists {
		return nil, errors.New("target database file segment missing")
	}

	val, err := targetFile.ReadValue(entry.valOffset, entry.valsize)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (db *DB) writeKeyDirEntry(key string, entry keyDirEntry) {
	db.muKeyDir.Lock()
	defer db.muKeyDir.Unlock()

	db.keyDir[key] = entry
}

func (db *DB) readKeyDirEntry(key string) (keyDirEntry, bool) {
	db.muKeyDir.RLock()
	defer db.muKeyDir.RUnlock()

	entry, exists := db.keyDir[key]

	return entry, exists
}

// Close ensures background processes wind down gracefully
func (db *DB) Close() error {
	db.cancelFunc()

	db.muFiles.Lock()
	defer db.muFiles.Unlock()

	for _, f := range db.files {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
