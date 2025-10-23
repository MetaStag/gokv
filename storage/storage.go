package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	debug "log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type Database interface {
	Close() error
	ScanDatabase(mp InMemoryMap) error
	UpdateDatabase(log Log) error
}

type InMemoryMap interface {
	GetValue(key string) string
	SetValue(key string, value string)
	DeleteValue(key string)
}

type Log interface {
	GetLSN() int
	GetCheckpoint() int
	SetLSN(a int)
	SetCheckpoint(a int)
	UpdateLog(operation string, key string, value string) (string, error)
}

type badgerDB struct {
	db    *badger.DB   // Database object
	mutex sync.RWMutex // Manage access to shared resources
}

type memStore struct {
	mp    map[string]string // In-memory map for fast access
	mutex sync.RWMutex      // Manage access to shared resources
}

type wal struct {
	lsn        int          // Keep track of log file entries
	checkpoint int          // Last checkpoint
	mutex      sync.RWMutex // Manage access to shared resources
}

// Start database connection
func InitDatabase() (Database, error) {
	db, err := badger.Open(badger.DefaultOptions("./db"))
	if err != nil {
		return nil, err
	}
	database := &badgerDB{db: db, mutex: sync.RWMutex{}}
	return database, nil
}

// Close Database connection before quitting
func (d *badgerDB) Close() error {
	err := d.db.Close()
	return err
}

// Load data from database to in-memory map
func (d *badgerDB) ScanDatabase(mp InMemoryMap) error {
	// Start a new transaction
	txn := d.db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10

	// Iterate over db
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		err := item.Value(func(val []byte) error {
			mp.SetValue(string(key), string(val))
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Reads from WAL log and updates database from last checkpoint
// Runs every 5 seconds
func (d *badgerDB) UpdateDatabase(log Log) error {
	// Open log file and save lines after checkpoint to array
	file, err := os.Open("wal.log")
	if err != nil {
		return err
	}

	var lines []string
	scanner := bufio.NewScanner(file)
	lineCount := 1
	checkpoint := log.GetCheckpoint()
	for scanner.Scan() {
		if lineCount < checkpoint { // ignore lines before checkpoint
			lineCount++
			continue
		}
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		file.Close()
		return err
	}
	file.Close()

	// If no new changes, return
	if len(lines) == 0 {
		return nil
	}

	// Iterate over each line and commit to database
	err = d.db.Update(func(txn *badger.Txn) error {
		for _, lineString := range lines {
			line := strings.Split(lineString, ",")
			if len(line) < 3 {
				debug.Println("Found invalid WAL entry - ", lineString)
				continue
			}
			if line[1] == "SET" {
				if err := txn.Set([]byte(line[2]), []byte(line[3])); err != nil {
					return err
				}

			} else if line[1] == "DELETE" {
				if err := txn.Delete([]byte(line[2])); err != nil {
					return err
				}
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Update checkpoint
	checkpoint = checkpoint + len(lines)
	log.SetCheckpoint(checkpoint)
	checkpointString := fmt.Sprintf("%d", checkpoint)

	// Save new checkpoint
	if err := os.WriteFile("checkpoint.txt", []byte(checkpointString), 0600); err != nil {
		return err
	}
	return nil
}

// Initialize In-memory map
func InitMap() InMemoryMap {
	return &memStore{mp: make(map[string]string), mutex: sync.RWMutex{}}
}

// Get value from in-memory map
func (m *memStore) GetValue(key string) string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mp[key]
}

// Set value in in-memory map
func (m *memStore) SetValue(key string, value string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mp[key] = value
}

// Delete value from in-memory map
func (m *memStore) DeleteValue(key string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.mp, key)
}

// Initialize Log
// Load the number of log file entries + checkpoint
func InitLog() (Log, error) {
	l := &wal{lsn: 0, checkpoint: 0, mutex: sync.RWMutex{}}

	// Open log file
	file, err := os.Open("wal.log")
	if err != nil {
		return nil, err
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
			return nil, err
		}
		count += bytes.Count(buf[:c], []byte{'\n'})
	}

	// Load last checkpoint
	checkpointBytes, err := os.ReadFile("checkpoint.txt")
	if err != nil {
		return nil, err
	}
	checkpointString := string(checkpointBytes)
	if checkpointString == "" {
		checkpointString = "0"
	}
	checkpointString = strings.TrimSpace(checkpointString)
	checkpointVal, err := strconv.Atoi(checkpointString)
	if err != nil {
		return nil, err
	}

	l.mutex.Lock()
	l.lsn = count + 1
	l.checkpoint = checkpointVal
	l.mutex.Unlock()

	return l, nil
}

// Get LSN value of Log
func (l *wal) GetLSN() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.lsn
}

// Set LSN value of Log
func (l *wal) SetLSN(a int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.lsn = a
}

// Get checkpoint value of Log
func (l *wal) GetCheckpoint() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.checkpoint
}

// Set checkpoint value of Log
func (l *wal) SetCheckpoint(a int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.checkpoint = a
}

// Write to log file
func (l *wal) UpdateLog(operation string, key string, value string) (string, error) {
	if operation != "SET" && operation != "DELETE" {
		return "", errors.New("Invalid operation to WAL log - " + operation)
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Format log entry
	var newLog string
	if operation == "SET" {
		newLog = fmt.Sprintf("%d,%s,%s,%s", l.lsn, operation, key, value)
	} else if operation == "DELETE" {
		newLog = fmt.Sprintf("%d,%s,%s", l.lsn, operation, key)
	}

	// Open log file
	file, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Write to log file
	_, err = file.WriteString(newLog + "\n")
	if err != nil {
		debug.Println("Could not write to WAL log - ", err)
		return "", err
	}

	// Update log file counter
	l.lsn++
	return newLog, nil
}
