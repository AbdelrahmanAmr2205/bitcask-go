package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/AbdelrahmanAmr2205/bitcask-go"
)

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if valueStr, exists := os.LookupEnv(key); exists {
		if val, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
			return val
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if valueStr, exists := os.LookupEnv(key); exists {
		if val, err := time.ParseDuration(valueStr); err == nil {
			return val
		}
	}
	return fallback
}

func main() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}

	// Environment variables configuration
	port := getEnv("PORT", "9092")
	dbDir := getEnv("BITCASK_DIR", homeDir+"/bitcask/data")
	maxActiveFileSize := getEnvInt64("BITCASK_MAX_FILE_SIZE", 4194304) // default 4MB
	compactInterval := getEnvDuration("BITCASK_COMPACT_INTERVAL", time.Hour)
	syncPeriod := getEnvDuration("BITCASK_SYNC_PERIOD", 0)
	expectedWriteRate := getEnvInt64("BITCASK_EXPECTED_WRITE_RATE", 16384)

	db, err := bitcask.InitDB(dbDir, maxActiveFileSize, compactInterval, syncPeriod, expectedWriteRate)
	if err != nil {
		log.Fatalf("Failed to initialize bitcask engine: %v", err)
	}
	defer db.Close()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Extract key from path (e.g. /my-key)
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			http.Error(w, "Key is required in the path", http.StatusBadRequest)
			return
		}

		key := path

		switch r.Method {
		case http.MethodGet:
			val, err := db.Get(key)
			if err != nil {
				if err.Error() == "Key does not exist" {
					http.Error(w, "Key not found", http.StatusNotFound)
					return
				}
				http.Error(w, fmt.Sprintf("Error retrieving key: %v", err), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(val)

		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Failed to read request body", http.StatusInternalServerError)
				return
			}
			defer r.Body.Close()

			if !json.Valid(body) {
				http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
				return
			}

			err = db.Put(key, body)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to save key: %v", err), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"status":"success"}`))

		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Printf("Starting bitcask-server on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
