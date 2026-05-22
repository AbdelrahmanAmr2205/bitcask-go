package bitcask_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/AbdelrahmanAmr2205/bitcask-go"
)

// Helper utility to instantiate an isolated temp directory for test containment
func setupTestDir(t *testing.T) string {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "bitcask_test_*")
	if err != nil {
		t.Fatalf("failed to build test directory container: %v", err)
	}
	return tmpDir
}

func TestBitcaskEngine_TableDriven(t *testing.T) {
	t.Run("CRUD Operations and Overwrites", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		db, err := bitcask.InitDB(dir, 1024*1024, 0, 0, 0)
		if err != nil {
			t.Fatalf("failed to initialize test database: %v", err)
		}
		defer db.Close()

		scenarios := []struct {
			name    string
			key     string
			val     []byte
			isWrite bool
			wantErr bool
		}{
			{"Write Key Foo", "foo", []byte("bar"), true, false},
			{"Write Key Alpha", "alpha", []byte("omega"), true, false},
			{"Read Key Foo", "foo", []byte("bar"), false, false},
			{"Overwrite Key Foo", "foo", []byte("new_bar"), true, false},
			{"Read Overwritten Key Foo", "foo", []byte("new_bar"), false, false},
			{"Read Non-Existent Key", "missing_key", nil, false, true},
		}

		for _, tc := range scenarios {
			t.Run(tc.name, func(t *testing.T) {
				if tc.isWrite {
					err := db.Put(tc.key, tc.val)
					if (err != nil) != tc.wantErr {
						t.Errorf("Put() error execution variance = %v, wantErr %v", err, tc.wantErr)
					}
				} else {
					got, err := db.Get(tc.key)
					if (err != nil) != tc.wantErr {
						t.Errorf("Get() error execution variance = %v, wantErr %v", err, tc.wantErr)
						return
					}
					if !tc.wantErr && !bytes.Equal(got, tc.val) {
						t.Errorf("Get() payload data payload mismatch: got = %s, want = %s", string(got), string(tc.val))
					}
				}
			})
		}
	})

	t.Run("Active File Size Rotation Limits (Milestone 3)", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		// Set a low max size threshold (30 bytes) to force rotation boundaries across sequential writes
		db, err := bitcask.InitDB(dir, 30, 0, 0, 0)
		if err != nil {
			t.Fatalf("failed initializing database rotation bounds: %v", err)
		}

		// Write three separate keys that will cross the 30-byte threshold limit
		keys := []string{"key_one", "key_two", "key_three"}
		value := []byte("telemetry_payload_bytes_long")

		for _, k := range keys {
			if err := db.Put(k, value); err != nil {
				t.Fatalf("failed appending to rotation test pool: %v", err)
			}
			// Briefly sleep to ensure the async channel pipeline completely processes disk tracking offsets
			time.Sleep(10 * time.Millisecond)
		}
		db.Close()

		// Verify that your engine successfully rotated files on disk
		files, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("failed verifying rotation directory state: %v", err)
		}

		var dataFilesCount int
		for _, f := range files {
			if filepath.Ext(f.Name()) == ".data" {
				dataFilesCount++
			}
		}

		// Because each append operation exceeded 30 bytes, the storage manager
		// should have triggered rotations, producing at least 2 distinct file segments on disk.
		if dataFilesCount < 2 {
			t.Errorf("rotation strategy failed: expected multiple segment log files, found only %d", dataFilesCount)
		}
	})

	t.Run("Crash Consistent State Recovery (Milestone 1 & 2)", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		// 1. Initialize instance and fill with data updates
		db, err := bitcask.InitDB(dir, 1024*1024, 0, 0, 0)
		if err != nil {
			t.Fatalf("failed starting recovery source layer: %v", err)
		}

		_ = db.Put("station_1", []byte("status_normal"))
		_ = db.Put("station_2", []byte("status_alert"))
		_ = db.Put("station_1", []byte("status_updated_critical")) // Overwrite state
		time.Sleep(10 * time.Millisecond)
		db.Close() // Simulate application clean crash shutdown

		// 2. Boot up a completely fresh engine instance targeting the same directory
		recoveredDB, err := bitcask.InitDB(dir, 1024*1024, 0, 0, 0)
		if err != nil {
			t.Fatalf("failed recovering storage directory bootstrap sequence: %v", err)
		}
		defer recoveredDB.Close()

		// 3. Confirm that the in-memory index was completely reconstructed from raw log sweeps
		val1, err := recoveredDB.Get("station_1")
		if err != nil || string(val1) != "status_updated_critical" {
			t.Errorf("recovery failed for station_1: got = %s, err = %v", string(val1), err)
		}

		val2, err := recoveredDB.Get("station_2")
		if err != nil || string(val2) != "status_alert" {
			t.Errorf("recovery failed for station_2: got = %s, err = %v", string(val2), err)
		}
	})

	t.Run("High Volume Concurrent Access Stresses", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		db, err := bitcask.InitDB(dir, 1024*1024, 0, 0, 0)
		if err != nil {
			t.Fatalf("failed starting concurrency test baseline: %v", err)
		}
		defer db.Close()

		var wg sync.WaitGroup
		workerCount := 20
		operationsPerWorker := 50

		// Spawn 20 parallel concurrent worker goroutines writing and reading from the engine simultaneously
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				key := "concurrent_key_" + string(rune(workerID))

				for j := 0; j < operationsPerWorker; j++ {
					_ = db.Put(key, []byte("payload_update"))
					_, _ = db.Get(key)
				}
			}(i)
		}

		wg.Wait() // Ensure zero data races or thread interlocks occur
	})
}

func TestBitcaskEngine_BackgroundCompaction(t *testing.T) {
	dir := setupTestDir(t)
	defer os.RemoveAll(dir)

	// Set compact interval low (100ms) with a tiny max file limit (25 bytes) to force rapid sweeps
	db, err := bitcask.InitDB(dir, 25, 100*time.Millisecond, 0, 50)
	if err != nil {
		t.Fatalf("failed initializing compaction target instance: %v", err)
	}

	// 1. Write the exact same key multiple times.
	// This generates numerous historical files containing stale data on disk.
	for i := 0; i < 10; i++ {
		err := db.Put("persistent_key", []byte(fmt.Sprintf("payload_update_index_%d", i)))
		if err != nil {
			t.Fatalf("failed writing log stream: %v", err)
		}
		time.Sleep(20 * time.Millisecond) // Let files rotate naturally bounds-wise
	}

	// 2. Sleep to allow the background ticker to fire and clean the directory
	time.Sleep(300 * time.Millisecond)

	// 3. Confirm the latest value remains perfectly readable
	got, err := db.Get("persistent_key")
	if err != nil {
		t.Fatalf("failed fetching key after compaction passes: %v", err)
	}

	if !bytes.Contains(got, []byte("payload_update_index_9")) {
		t.Errorf("compaction state corruption: expected latest payload, got %s", string(got))
	}
	db.Close()

	// 4. Verify disk space recovery: the dozens of initial files should be collapsed
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed inspecting post-merge layout: %v", err)
	}

	var dataFilesCount int
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".data" {
			dataFilesCount++
		}
	}

	// Out of the 10 rotated segments, compaction should have collapsed the historic layers,
	// leaving a heavily minimized set of optimized file entries on disk.
	if dataFilesCount > 4 {
		t.Errorf("garbage collection failed to shrink data logs properly: found %d remaining data files", dataFilesCount)
	}
}

func TestBitcaskEngine_HintFileBoot(t *testing.T) {
	dir := setupTestDir(t)
	defer os.RemoveAll(dir)

	// Set compact interval low (100ms) with a tiny max file limit (25 bytes) to force rapid sweeps
	db, err := bitcask.InitDB(dir, 25, 100*time.Millisecond, 0, 50)
	if err != nil {
		t.Fatalf("failed initializing compaction target instance: %v", err)
	}

	// Write key-values to trigger rotation and compaction
	for i := 0; i < 10; i++ {
		err := db.Put("hint_key", []byte(fmt.Sprintf("payload_%d", i)))
		if err != nil {
			t.Fatalf("failed writing log stream: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for compaction to generate .hint files
	time.Sleep(300 * time.Millisecond)
	db.Close()

	// Verify hint files exist
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed inspecting post-compaction layout: %v", err)
	}

	var hintFilesCount int
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".hint" {
			hintFilesCount++
		}
	}

	if hintFilesCount == 0 {
		t.Fatalf("compaction failed to produce .hint files")
	}

	// Boot up a fresh engine instance to read from the generated hint files
	recoveredDB, err := bitcask.InitDB(dir, 25, 100*time.Millisecond, 0, 50)
	if err != nil {
		t.Fatalf("failed recovering database from hint files: %v", err)
	}
	defer recoveredDB.Close()

	// Confirm that the latest value is available and correct
	got, err := recoveredDB.Get("hint_key")
	if err != nil {
		t.Fatalf("failed fetching key after hint boot: %v", err)
	}

	expected := []byte("payload_9")
	if !bytes.Equal(got, expected) {
		t.Errorf("hint recovery mismatch: got %s, want %s", string(got), expected)
	}
}
