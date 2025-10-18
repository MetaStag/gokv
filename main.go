package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

var (
	db    *badger.DB        // Database object
	c     *http.Client      // HTTP Client to talk to other nodes
	nodes []string          // List of connected nodes
	lsn   int               // Keep track of log file entries
	mp    map[string]string // In-memory map for fast access
	mutex sync.RWMutex      // Manage access to shared resources
)

// Check if important file/folders exist, if not then create them
// Future scalability: represent the files/folders in an array, to reduce code
func ValidateFiles() bool {
	_, err := os.Stat("wal.log")
	if os.IsNotExist(err) {
		log.Println("Log file does not exist, creating one")
		file, err := os.Create("wal.log")
		if err != nil {
			log.Println("Could not create log file - ", err)
			return false
		}
		file.Close()
	}
	_, err = os.Stat("checkpoint.txt")
	if os.IsNotExist(err) {
		log.Println("Checkpoint file does not exist, creating one")
		file, err := os.Create("checkpoint.txt")
		file.WriteString("0")
		if err != nil {
			log.Println("Could not create checkpoint file - ", err)
			return false
		}
		file.Close()
	}
	_, err = os.Stat("./db")
	if os.IsNotExist(err) {
		log.Println("Database folder does not exist, creating one")
		err = os.Mkdir("./db", 0600)
		if err != nil {
			log.Println("Could not create db folder - ", err)
			return false
		}
	}
	return true
}

// Load the number of log entries for checkpoint purpose
func LoadLSN() (int, error) {
	file, err := os.Open("wal.log")
	if err != nil {
		return 0, err
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
			return count, err
		}
		count += bytes.Count(buf[:c], []byte{'\n'})
	}
	return count, nil
}

// Load data from database to in-memory map
func ScanDatabase() error {
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

// Reads from WAL log and updates database from last checkpoint
// Runs every 5 seconds
func UpdateDatabase() error {
	// Load last checkpoint
	checkpointBytes, err := os.ReadFile("checkpoint.txt")
	if err != nil {
		return err
	}
	checkpointString := string(checkpointBytes)
	checkpoint, err := strconv.Atoi(checkpointString)
	if err != nil {
		return err
	}

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
		if lineCount <= checkpoint { // ignore lines before checkpoint
			lineCount++
			continue
		}
		lineString := scanner.Text()
		lines = append(lines, lineString)
	}
	file.Close()
	mutex.RUnlock()

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

	// Make new checkpoint from log file
	checkpoint, err = LoadLSN()
	if err != nil {
		return err
	}
	checkpointString = fmt.Sprintf("%d", checkpoint)

	// Save new checkpoint
	if err := os.WriteFile("checkpoint.txt", []byte(checkpointString), 0600); err != nil {
		return err
	}
	return nil
}

// Write to log file
func UpdateLog(operation string, key string, value string) error {
	// Open log file
	file, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	// Format log entry
	var newLog string
	if operation == "SET" {
		newLog = fmt.Sprintf("%d,%s,%s,%s\n", lsn, operation, key, value)
	} else if operation == "DELETE" {
		newLog = fmt.Sprintf("%d,%s,%s\n", lsn, operation, key)
	} else {
		return errors.New("Invalid operation to WAL log - " + operation)
	}

	// Write to log file
	_, err = file.WriteString(newLog)
	if err != nil {
		log.Println("Could not write to WAL log - ", err)
		return err
	}

	// Propagate change to other nodes
	bodyFormat := fmt.Sprintf("{update: %s}", newLog)
	body := strings.NewReader(bodyFormat)

	for _, v := range nodes {
		resp, err := c.Post(v+"/internal/update", "application/json", body)
		if err != nil {
			log.Println("Could not send changes to node - ", err)
		}
		resp.Body.Close()
	}

	lsn++ // update log file counter
	return nil
}

// Helper function for returning HTTP response
func WriteResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-type", "Application/json") // JSON response
	w.WriteHeader(statusCode)                          // add HTTP status code

	// Format JSON and add it to response body
	resp := make(map[string]string)
	resp["message"] = message
	json.NewEncoder(w).Encode(resp)
}

// Check health of node
func HealthCheck(w http.ResponseWriter, r *http.Request) {
	WriteResponse(w, 200, "OK")
}

// Run this function initially to connect to other nodes
// It finds the IP of other nodes from cluster.txt
func ConnectNodes() error {
	// Load data from cluster.txt
	data, err := os.ReadFile("cluster.txt")
	if err != nil {
		return err
	}

	// Update nodes[]
	lines := bytes.Lines(data)
	for i := range lines {
		nodes = append(nodes, string(i))
	}
	return nil
}

// Ping other nodes to check if connection is alive
// Updates nodes[] if a connection breaks
func Ping() error {
	var newNodes []string
	for _, v := range nodes {
		resp, err := c.Get(v + "/ping")
		if err != nil || resp.Status != "200 OK" {
			continue
		} else {
			newNodes = append(newNodes, v)
		}
	}
	nodes = newNodes
	return nil
}

// Recieve and mark WAL updates from other nodes
func InternalUpdateRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		WriteResponse(w, 400, "Invalid HTTP Method")
		return
	}

	// Read request body
	b := make([]byte, 32)
	_, err := r.Body.Read(b)
	if err != nil {
		log.Println("Could not read POST body - ", err)
		WriteResponse(w, 400, "Invalid request body")
		return
	}
	newLog := string(b)

	mutex.Lock()
	defer mutex.Unlock()

	// Open log file
	file, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		log.Println("Could not write to WAL log - ", err)
		WriteResponse(w, 400, "Internal Server Error")
		return
	}
	defer file.Close()

	// Write to log file
	_, err = file.WriteString(newLog)
	if err != nil {
		log.Println("Could not write to WAL log - ", err)
		WriteResponse(w, 400, "Internal Server Error")
	}
}

// Fetch value from key
func GetRequest(w http.ResponseWriter, r *http.Request) {
	// Validate HTTP method
	if r.Method != "GET" {
		WriteResponse(w, 400, "Invalid HTTP Method")
		return
	}

	// Extract Query Parameter
	KeyQuery := r.URL.Query()["key"]
	if len(KeyQuery) == 0 {
		WriteResponse(w, 404, "Key not found")
		return
	}

	// Extract Key
	key := KeyQuery[0]

	// Read value from map
	mutex.RLock()
	value := mp[key]
	mutex.RUnlock()

	// Return value
	if value != "" {
		WriteResponse(w, 200, value)
	} else {
		WriteResponse(w, 404, "Value Not found")
	}
}

// Save key-value pair
func SetRequest(w http.ResponseWriter, r *http.Request) {
	// Validate HTTP method
	if r.Method != "GET" {
		WriteResponse(w, 400, "Invalid HTTP Method")
		return
	}

	// Extract Query Parameters
	KeyQuery := r.URL.Query()["key"]
	ValueQuery := r.URL.Query()["value"]
	if len(KeyQuery) == 0 {
		WriteResponse(w, 404, "Key not found")
		return
	} else if len(ValueQuery) == 0 {
		WriteResponse(w, 404, "Value not found")
		return
	}

	// Extract Key and Value
	key := KeyQuery[0]
	value := ValueQuery[0]
	if len(key) > 50 {
		WriteResponse(w, 400, "Key length too long")
		return
	} else if len(value) > 100 {
		WriteResponse(w, 400, "Value length too long")
		return
	}

	// Save to map and database
	mutex.Lock()

	mp[key] = value
	err := UpdateLog("SET", key, value)

	if err != nil {
		log.Println("Error writing to log - ", err)
		WriteResponse(w, 400, "Internal Server Error")
		defer os.Exit(1) // quit if WAL is not working
	} else {
		WriteResponse(w, 200, "Key saved")
	}
	mutex.Unlock()
}

// Delete key-value pair
func DeleteRequest(w http.ResponseWriter, r *http.Request) {
	// Validate HTTP method
	if r.Method != "GET" {
		WriteResponse(w, 400, "Invalid HTTP Method")
		return
	}

	// Extract Query Paramter
	KeyQuery := r.URL.Query()["key"]
	if len(KeyQuery) == 0 {
		WriteResponse(w, 404, "Key not found")
		return
	}

	// Extract key
	key := KeyQuery[0]

	// Delete key-value from map and database
	mutex.Lock()
	delete(mp, key)
	err := UpdateLog("DELETE", key, "")

	if err != nil {
		log.Println("Error writing to log - ", err)
		WriteResponse(w, 400, "Internal Server Error")
		defer os.Exit(1) // quit if WAL is not working
	} else {
		WriteResponse(w, 200, "Key deleted")
	}
	mutex.Unlock()
}

func main() {
	// Check if all required files exist
	if !ValidateFiles() {
		log.Fatal("Necessary files don't exist, Exiting")
	}

	// Start database connection
	var err error
	db, err = badger.Open(badger.DefaultOptions("./db"))
	if err != nil {
		log.Fatal("Error opening database - ", err)
	}
	defer db.Close()

	// Initialize in-memory map and load values
	mp = make(map[string]string)

	if err := ScanDatabase(); err != nil {
		log.Fatal("Error loading data from database - ", err)
	}

	// Load last checkpoint
	lsn, err = LoadLSN()
	if err != nil {
		log.Fatal("Could not load LSN")
	}
	lsn++

	// Update database every 5 seconds
	go func() {
		for {
			time.Sleep(time.Second * 5)
			log.Println("Running update database goroutine") // for debugging, remove later
			err = UpdateDatabase()
			if err != nil {
				log.Fatal("Error saving to database - ", err)
			}
		}
	}()

	// Connect to other nodes
	err = ConnectNodes()
	if err != nil {
		log.Fatal("Could not connect to other nodes")
	}

	// Periodically ping nodes to check if connection is alive
	c = &http.Client{}
	go func() {
		for {
			time.Sleep(time.Minute * 2)
			log.Println("Running ping goroutine") // for debugging, remove later
			err := Ping()
			if err != nil {
				log.Fatal("Error pinging other nodes, - ", err)
			}
		}
	}()

	// Define port on which server will run
	PORT := ":8080"

	// Define Routes
	http.HandleFunc("/ping", HealthCheck)
	http.HandleFunc("/get", GetRequest)
	http.HandleFunc("/set", SetRequest)
	http.HandleFunc("/delete", DeleteRequest)
	http.HandleFunc("/internal/update", InternalUpdateRequest)

	// Start Server
	log.Printf("Server running on http://localhost%s\n", PORT)
	log.Fatal(http.ListenAndServe(PORT, nil))
}
