package api

import (
	h "key/helper"
	"key/storage"
	"log"
	"net/http"
	"os"
)

// Fetch value from key
func GetRequest(w http.ResponseWriter, r *http.Request) {
	// Validate HTTP method
	if r.Method != "GET" {
		h.WriteResponse(w, 400, "Invalid HTTP Method")
		return
	}

	// Extract Query Parameter
	KeyQuery := r.URL.Query()["key"]
	if len(KeyQuery) == 0 {
		h.WriteResponse(w, 404, "Key not found")
		return
	}

	// Extract Key
	key := KeyQuery[0]

	// Read value from storage
	value := storage.GetValue(key)

	// Return value
	if value != "" {
		h.WriteResponse(w, 200, value)
	} else {
		h.WriteResponse(w, 404, "Value Not found")
	}
}

// Save key-value pair
func SetRequest(w http.ResponseWriter, r *http.Request) {
	// Validate HTTP method
	if r.Method != "GET" {
		h.WriteResponse(w, 400, "Invalid HTTP Method")
		return
	}

	// Extract Query Parameters
	KeyQuery := r.URL.Query()["key"]
	ValueQuery := r.URL.Query()["value"]
	if len(KeyQuery) == 0 {
		h.WriteResponse(w, 404, "Key not found")
		return
	} else if len(ValueQuery) == 0 {
		h.WriteResponse(w, 404, "Value not found")
		return
	}

	// Extract Key and Value
	key := KeyQuery[0]
	value := ValueQuery[0]
	if len(key) > 50 {
		h.WriteResponse(w, 400, "Key length too long")
		return
	} else if len(value) > 100 {
		h.WriteResponse(w, 400, "Value length too long")
		return
	}

	// Save key-value to storage
	err := storage.SetValue(key, value)

	if err != nil {
		log.Println("Error writing to log - ", err)
		h.WriteResponse(w, 400, "Internal Server Error")
		defer os.Exit(1) // quit if WAL is not working
	} else {
		h.WriteResponse(w, 200, "Key saved")
	}
}

// Delete key-value pair
func DeleteRequest(w http.ResponseWriter, r *http.Request) {
	// Validate HTTP method
	if r.Method != "GET" {
		h.WriteResponse(w, 400, "Invalid HTTP Method")
		return
	}

	// Extract Query Paramter
	KeyQuery := r.URL.Query()["key"]
	if len(KeyQuery) == 0 {
		h.WriteResponse(w, 404, "Key not found")
		return
	}

	// Extract key
	key := KeyQuery[0]

	// Delete key-value from storage
	err := storage.DeleteValue(key)

	if err != nil {
		log.Println("Error writing to log - ", err)
		h.WriteResponse(w, 400, "Internal Server Error")
		defer os.Exit(1) // quit if WAL is not working
	} else {
		h.WriteResponse(w, 200, "Key deleted")
	}
}
