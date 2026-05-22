package bitcask

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/AbdelrahmanAmr2205/bitcask-go/internal/datafile"
)

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

	err = db.buildKeyDir()
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
			return fmt.Errorf("failed to bootstrap initial active file: %w", err)
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

	db.muFiles.Lock()
	db.activeFile = db.files[maxFileID]
	db.muFiles.Unlock()

	return nil
}

func (db *DB) buildKeyDir() error {
	db.muFiles.RLock()
	defer db.muFiles.RUnlock()

	var sortedIds []int
	for id := range db.files {
		sortedIds = append(sortedIds, id)
	}
	slices.Sort(sortedIds)

	for _, id := range sortedIds {
		entries, err := db.files[id].ScanRecords()
		if err != nil {
			return fmt.Errorf("failed to recover data segment %d: %w", id, err)
		}

		for _, entry := range entries {
			currentEntry, exists := db.readKeyDirEntry(entry.Key)

			if exists && currentEntry.timestamp > entry.Timestamp {
				continue
			}

			db.writeKeyDirEntry(entry.Key, keyDirEntry{
				valsize:   entry.ValSize,
				valOffset: entry.ValOffset,
				fileID:    id,
				timestamp: entry.Timestamp,
			})
		}
	}

	return nil
}
