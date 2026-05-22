package bitcask

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/AbdelrahmanAmr2205/bitcask-go/internal/datafile"
)

func (db *DB) RunCompactionLoop(ctx context.Context) {
	if db.config.CompactInterval <= 0 {
		return
	}

	ticker := time.NewTicker(db.config.CompactInterval)
	defer ticker.Stop()

	// Track baseline stats for adaptive runtime tuning
	var lastWriteOffset int64 = 0
	db.muFiles.RLock()
	if db.activeFile != nil {
		lastWriteOffset = db.activeFile.Size()
	}
	db.muFiles.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 1. Recalibrate adaptive configuration metrics before running the pass
			batch := db.getCompactionBatch()

			db.muFiles.RLock()
			activeFile := db.activeFile
			db.muFiles.RUnlock()

			if activeFile != nil {
				bytesWritten := int64(len(batch)) * db.config.MaxActiveFileSize
				durationSec := db.config.CompactInterval.Seconds()
				currentOffset := activeFile.Size()
				var estimatedRate int64

				if bytesWritten > 0 {
					// Adaptively estimate writes per second based on recent history
					estimatedRate = int64(float64(bytesWritten) / durationSec)

				} else if bytesWritten == 0 {
					bytesWritten = currentOffset - lastWriteOffset
					estimatedRate = int64(float64(bytesWritten) / durationSec)
				}

				if estimatedRate > 0 {
					db.config.ExpectedWriteRate = estimatedRate
					// Recompute files generated per interval
					totalExpectedBytes := float64(db.config.ExpectedWriteRate) * durationSec
					computedFiles := int(math.Ceil(totalExpectedBytes / float64(db.config.MaxActiveFileSize)))

					// Keep a healthy window size minimum for quick catching up
					if computedFiles < 4 {
						computedFiles = 4
					}
					db.config.ExpectedFilesPerInterval = computedFiles
				}
				lastWriteOffset = currentOffset
			}

			// 2. Fetch the bounded selection batch
			if len(batch) < 2 {
				continue
			}

			// 3. Execute the safe merge compaction pass
			if err := db.mergeBatch(batch); err != nil {
				continue
			}
		}
	}
}

func (db *DB) getCompactionBatch() []int {
	db.muFiles.RLock()
	activeID := db.activeFile.ID()

	var immutableIDs []int
	for id := range db.files {
		if id < activeID {
			immutableIDs = append(immutableIDs, id)
		}
	}
	db.muFiles.RUnlock()

	if len(immutableIDs) < 2 {
		return nil // Requires at least 2 files to justify a merge pass
	}

	sort.Ints(immutableIDs)

	// Dynamically establish our maximum safe window cap
	maxBatchSize := int(db.config.ExpectedFilesPerInterval)
	if maxBatchSize < 2 {
		maxBatchSize = 2
	}
	if maxBatchSize > 16 {
		maxBatchSize = 16 // Enforce a hard upper ceiling to prevent I/O saturation
	}

	if len(immutableIDs) > maxBatchSize {
		return immutableIDs[:maxBatchSize]
	}

	return immutableIDs
}

func (db *DB) mergeBatch(batch []int) error {
	// The new consolidated file name targets the highest segment ID within this specific merge block
	highestID := batch[len(batch)-1]
	mergeFileName := fmt.Sprintf("%010d.merge", highestID)
	mergeFilePath := filepath.Join(db.config.Directory, mergeFileName)

	mergeHintFileName := fmt.Sprintf("%010d.merge-hint", highestID)
	mergeHintFilePath := filepath.Join(db.config.Directory, mergeHintFileName)

	// Initialize an independent, temporary append-only file handler for the compilation merge pass
	mergeFile, err := os.OpenFile(mergeFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp compaction file: %w", err)
	}

	mergeHintFile, err := os.OpenFile(mergeHintFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		_ = mergeFile.Close()
		return fmt.Errorf("failed to open temp hint file: %w", err)
	}

	// Track internal offsets inside our temporary file layout
	var currentWriteOffset int64 = 0
	type pendingKeyUpdate struct {
		key   string
		entry keyDirEntry
	}
	var updates []pendingKeyUpdate

	// 1. Linearly sweep records from the target batch completely free of locks
	for _, id := range batch {
		db.muFiles.RLock()
		file, exists := db.files[id]
		db.muFiles.RUnlock()
		if !exists {
			_ = mergeHintFile.Close()
			_ = mergeFile.Close()
			return fmt.Errorf("file segment missing mid-compaction pass")
		}

		scanned, err := file.ScanRecords()
		if err != nil {
			_ = mergeHintFile.Close()
			_ = mergeFile.Close()
			return err
		}

		for _, entry := range scanned {
			// Check liveness against our KeyDir using non-blocking shared Read Locks
			db.muKeyDir.RLock()
			currentMemEntry, exists := db.keyDir[entry.Key]
			db.muKeyDir.RUnlock()

			// CRITICAL METRIC: If the memory pointer tracks this exact file segment and offset location,
			// it represents the true live state of the weather station. It must be preserved.
			if exists && currentMemEntry.fileID == id && currentMemEntry.valOffset == entry.ValOffset {
				// Re-read full raw value bytes safely from the historical file layer
				valBytes, err := file.ReadValue(entry.ValOffset, entry.ValSize)
				if err != nil {
					_ = mergeHintFile.Close()
					_ = mergeFile.Close()
					return err
				}

				// Serialize and write directly into our temp .merge log
				record := datafile.NewRecord(entry.Key, valBytes, entry.Timestamp)
				encoded, err := record.Encode()
				if err != nil {
					_ = mergeHintFile.Close()
					_ = mergeFile.Close()
					return err
				}

				if _, err := mergeFile.Write(encoded); err != nil {
					_ = mergeHintFile.Close()
					_ = mergeFile.Close()
					return err
				}

				valOffset := currentWriteOffset + int64(datafile.HeaderSize) + int64(len(entry.Key))

				hintRecord := datafile.NewHintRecord(entry.Timestamp, uint32(len(entry.Key)), entry.ValSize, valOffset, entry.Key)
				hintEncoded, err := hintRecord.Encode()
				if err != nil {
					_ = mergeHintFile.Close()
					_ = mergeFile.Close()
					return err
				}

				if _, err := mergeHintFile.Write(hintEncoded); err != nil {
					_ = mergeHintFile.Close()
					_ = mergeFile.Close()
					return err
				}

				// Queue up memory pointer recalculations to apply atomically at the end of our loop
				updates = append(updates, pendingKeyUpdate{
					key: entry.Key,
					entry: keyDirEntry{
						valsize:   entry.ValSize,
						valOffset: valOffset,
						fileID:    highestID, // Target consolidation ID
						timestamp: entry.Timestamp,
					},
				})

				currentWriteOffset += int64(len(encoded))
			}
			// If it does not point here, it means a newer item was written via Put() over Kafka.
			// It is safely discarded (Garbage Collected).
		}
	}

	if err := mergeFile.Sync(); err != nil {
		_ = mergeHintFile.Close()
		_ = mergeFile.Close()
		return err
	}
	_ = mergeFile.Close()

	if err := mergeHintFile.Sync(); err != nil {
		_ = mergeHintFile.Close()
		return err
	}
	_ = mergeHintFile.Close()

	// 2. ATOMIC SWAP PHASE: Acquire short, exclusive locks to map memory states
	db.muFiles.Lock()
	db.muKeyDir.Lock()

	if oldFile, ok := db.files[highestID]; ok {
		_ = oldFile.Close()
	}

	// Open our newly consolidated permanent .data log file segment
	finalDataPath := filepath.Join(db.config.Directory, fmt.Sprintf("%010d.data", highestID))
	finalHintPath := filepath.Join(db.config.Directory, fmt.Sprintf("%010d.hint", highestID))

	if err := os.Rename(mergeFilePath, finalDataPath); err != nil {
		db.muKeyDir.Unlock()
		db.muFiles.Unlock()
		return fmt.Errorf("failed converting merge log to data segment: %w", err)
	}

	if err := os.Rename(mergeHintFilePath, finalHintPath); err != nil {
		db.muKeyDir.Unlock()
		db.muFiles.Unlock()
		return fmt.Errorf("failed converting merge hint log to hint segment: %w", err)
	}

	finalDataFile, err := datafile.OpenDataFile(db.config.Directory, highestID)
	if err != nil {
		db.muKeyDir.Unlock()
		db.muFiles.Unlock()
		return err
	}

	// Apply the updated pointers directly to the active KeyDir map
	for _, up := range updates {
		// Double-check edge-case: Did a Put() arrive over the network while we were copying bytes?
		if current, exists := db.keyDir[up.key]; exists && current.timestamp > up.entry.timestamp {
			continue
		}
		db.keyDir[up.key] = up.entry
	}

	// Securely untrack stale file descriptors from our active registry loops
	var filesToDelete []string
	for _, id := range batch {
		if id == highestID {
			db.files[id] = finalDataFile
			continue
		}

		if f, ok := db.files[id]; ok {
			_ = f.Close()
			delete(db.files, id)
			filesToDelete = append(filesToDelete, filepath.Join(db.config.Directory, fmt.Sprintf("%010d.data", id)))
			filesToDelete = append(filesToDelete, filepath.Join(db.config.Directory, fmt.Sprintf("%010d.hint", id)))
		}
	}

	db.muKeyDir.Unlock()
	db.muFiles.Unlock()

	// 3. Clear file system space completely outside critical lock paths
	for _, fPath := range filesToDelete {
		_ = os.Remove(fPath)
	}

	return nil
}
