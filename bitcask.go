package bitcask

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
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
	Directory         string        // Path to the data directory on disk
	MaxActiveFileSize int64         // Max size in bytes before rotating the active file
	CompactInterval   time.Duration // Time window between background compaction merges
	SyncPeriod        time.Duration // Optional background interval to force fsync (0 means disabled)
}

func (db *DB) startWriteLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			//TODO: add draining logic before terminating
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
		}
	}
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
	entry, err := db.readKeyDirEntry(key)
	if err != nil {
		return nil, err
	}

	db.muFiles.RLock()
	targetFile, fileExists := db.files[entry.fileID]
	db.muFiles.RUnlock()

	if !fileExists {
		return nil, errors.New("target database file segment missing")
	}

	val, err := targetFile.ReadRecord(entry.valOffset, entry.valsize)
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

func (db *DB) readKeyDirEntry(key string) (keyDirEntry, error) {
	db.muKeyDir.RLock()
	defer db.muKeyDir.RUnlock()

	entry, exists := db.keyDir[key]
	if !exists {
		return keyDirEntry{}, errors.New("Key does not exist")
	}

	return entry, nil
}

func InitDB(directoryPath string, maxActiveFileSize int64, compactInterval time.Duration, syncPeriod time.Duration) (*DB, error) {
	ctx, cancel := context.WithCancel(context.Background())

	config := Config{
		Directory:         directoryPath,
		MaxActiveFileSize: maxActiveFileSize,
		SyncPeriod:        syncPeriod,
		CompactInterval:   compactInterval,
	}

	db := &DB{
		keyDir:     make(map[string]keyDirEntry),
		writeChan:  make(chan writeRequest, 100),
		files:      make(map[int]*datafile.DataFile),
		config:     config,
		cancelFunc: cancel,
	}

	err := db.loadFiles()
	if err != nil {
		return nil, err
	}

	go db.startWriteLoop(ctx)

	return db, nil
}

func (db *DB) loadFiles() error {
	dirEntries, err := os.ReadDir(db.config.Directory)
	if err != nil {
		return err
	}

	if len(dirEntries) == 0 {
		activeFile, err := datafile.OpenDataFile(db.config.Directory, 1)
		if err != nil {
			return err
		}
		db.muFiles.Lock()
		db.files[1] = activeFile
		db.activeFile = activeFile
		db.muFiles.Unlock()
		return nil
	}

	maxFileID := 0
	for _, dirEntry := range dirEntries {
		fileID, err := strconv.Atoi(strings.TrimSuffix(dirEntry.Name(), ".data"))
		if err != nil {
			return err
		}

		if fileID > maxFileID {
			maxFileID = fileID
		}

		db.muFiles.Lock()
		db.files[fileID], err = datafile.OpenDataFile(db.config.Directory, fileID)
		db.muFiles.Unlock()

		if err != nil {
			return err
		}
	}

	db.muFiles.RLock()
	db.activeFile = db.files[maxFileID]
	db.muFiles.RUnlock()

	return nil
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
