package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"key/network"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

var (
	db         *badger.DB        // Database object
	lsn        int               // Keep track of log file entries
	checkpoint int               // Last checkpoint
	mp         map[string]string // In-memory map for fast access
	mutex      sync.RWMutex      // Manage access to shared resources
)

// Initialize Storage
// Start database connection, in-memory map and load checkpoint values
func InitStorage() error {
	// Initialize database connection
	var err error
	db, err = badger.Open(badger.DefaultOptions("./db"))
	if err != nil {
		return err
	}

	// Initialize in-memory map and load values
	mp = make(map[string]string)

	// Load values from database to in-memory map
	err = scanDatabase()
	if err != nil {
		return err
	}

	// Load last checkpoint
	err = loadCheckpoint()
	if err != nil {
		return err
	}
	return nil
}

// Close Database connection before quitting
func CloseStorage() {
	db.Close()
}

// Get value from in-memory map
func GetValue(key string) string {
	mutex.RLock()
	defer mutex.RUnlock()
	return mp[key]
}

// Set value in in-memory map + update log file
func SetValue(key string, value string) error {
	mutex.Lock()
	defer mutex.Unlock()
	mp[key] = value
	newLog, err := updateLog("SET", key, value)
	if err != nil {
		return err
	}

	// Propagate change to other nodes
	err = network.PropagateChange(newLog)
	if err != nil {
		log.Println("Could not propagate change to other nodes - ", err)
		return err
	}
	return nil
}

// Delete value from in-memory map + update log file
func DeleteValue(key string) error {
	mutex.Lock()
	defer mutex.Unlock()
	delete(mp, key)
	newLog, err := updateLog("DELETE", key, "")
	if err != nil {
		return err
	}

	// Propagate change to other nodes
	err = network.PropagateChange(newLog)
	if err != nil {
		log.Println("Could not propagate change to other nodes - ", err)
		return err
	}
	return nil
}

// Load the number of log file entries + checkpoint
func loadCheckpoint() error {
	mutex.RLock()
	defer mutex.RUnlock()
	file, err := os.Open("wal.log")
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 32*1024) // make a 32kb buffer
	count := 0

	// Read the file in chunks and count all line breaks
	for {
		c, err := file.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		count += bytes.Count(buf[:c], []byte{'\n'})
	}
	lsn = count + 1

	// Load last checkpoint
	checkpointBytes, err := os.ReadFile("checkpoint.txt")
	if err != nil {
		return err
	}
	checkpointString := string(checkpointBytes)
	checkpoint, err = strconv.Atoi(checkpointString)
	if err != nil {
		return err
	}

	return nil
}

// Reads from WAL log and updates database from last checkpoint
// Runs every 5 seconds
func UpdateDatabase() error {
	// Open log file and save lines after checkpoint to array
	mutex.RLock()
	file, err := os.Open("wal.log")
	if err != nil {
		mutex.RUnlock()
		return err
	}

	var lines []string
	scanner := bufio.NewScanner(file)
	lineCount := 1
	for scanner.Scan() {
		if lineCount < checkpoint { // ignore lines before checkpoint
			lineCount++
			continue
		}
		lineString := scanner.Text()
		lines = append(lines, lineString)
	}
	file.Close()
	mutex.RUnlock()

	// If no new changes, return
	if len(lines) == 0 {
		return nil
	}

	// Iterate over each line and commit to database
	for _, lineString := range lines {
		line := strings.Split(lineString, ",")

		err := db.Update(func(txn *badger.Txn) error {
			if line[1] == "SET" {
				if err := txn.Set([]byte(line[2]), []byte(line[3])); err != nil {
					return err
				}

			} else if line[1] == "DELETE" {
				if err := txn.Delete([]byte(line[2])); err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			return err
		}
	}

	// Update checkpoint
	checkpoint = lsn
	checkpointString := fmt.Sprintf("%d", checkpoint)

	// Save new checkpoint
	if err := os.WriteFile("checkpoint.txt", []byte(checkpointString), 0600); err != nil {
		return err
	}
	return nil
}

// Load data from database to in-memory map
func scanDatabase() error {
	// Start a new transaction
	txn := db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10

	// Iterate over db
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		err := item.Value(func(val []byte) error {
			mp[string(key)] = string(val)
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Write to log file
func updateLog(operation string, key string, value string) (string, error) {
	// Open log file
	file, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Format log entry
	var newLog string
	if operation == "SET" {
		newLog = fmt.Sprintf("%d,%s,%s,%s", lsn, operation, key, value)
	} else if operation == "DELETE" {
		newLog = fmt.Sprintf("%d,%s,%s", lsn, operation, key)
	} else {
		return "", errors.New("Invalid operation to WAL log - " + operation)
	}

	// Write to log file
	_, err = file.WriteString(newLog + "\n")
	if err != nil {
		log.Println("Could not write to WAL log - ", err)
		return "", err
	}

	// Update log file counter
	lsn++
	return newLog, nil
}
