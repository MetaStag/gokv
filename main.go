package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
)

// In-memory map for fast access
// potential issue: too much data will overload memory
var (
	db *badger.DB
	mp map[string]string
	mutex sync.RWMutex
)

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

// Helper function for returning HTTP response
func WriteResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-type", "Application/json") // JSON response
	w.WriteHeader(statusCode) // add HTTP status code

	// Format JSON and add it to response body
	resp := make(map[string]string)
	resp["message"] = message
	json.NewEncoder(w).Encode(resp)
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
	if len(KeyQuery)==0 {
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
	if len(KeyQuery) == 0  {
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
	err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(key), []byte(value)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Println("Error saving to database - ", err)
		WriteResponse(w, 400, "Internal Server Error")
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
	err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(key)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Println("Error deleting from database - ", err)
		WriteResponse(w, 400, "Internal Server Error")
	} else {
		WriteResponse(w, 200, "Key deleted")
	}
	mutex.Unlock()
}

func main() {
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

	// Define port on which server will run
	PORT := ":8080"

	// Define Routes
	http.HandleFunc("/get", GetRequest)
	http.HandleFunc("/set", SetRequest)
	http.HandleFunc("/delete", DeleteRequest)

	// Start Server
	log.Printf("Server running on http://localhost%s\n", PORT)
	log.Fatal(http.ListenAndServe(PORT, nil))
}