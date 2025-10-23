package api

import (
	"encoding/json"
	h "gokv/helper"
	"gokv/storage"
	"io"
	"log"
	"net/http"
)

type Server struct {
	mp  storage.InMemoryMap
	log storage.Log
}

func New(m storage.InMemoryMap, l storage.Log) *Server {
	return &Server{mp: m, log: l}
}

// Check health of node
func HealthCheck(w http.ResponseWriter, r *http.Request) {
	h.WriteResponse(w, 200, "OK")
}

// Fetch value from key
func (s *Server) GetRequest(w http.ResponseWriter, r *http.Request) {
	// Validate HTTP method
	if r.Method != "GET" {
		h.WriteResponse(w, http.StatusMethodNotAllowed, "Invalid HTTP Method")
		return
	}

	// Extract Query Parameter
	key := r.URL.Query().Get("key")
	if key == "" {
		h.WriteResponse(w, 404, "Key not found")
		return
	}

	// Read value from storage
	value := s.mp.GetValue(key)

	// Return value
	if value != "" {
		h.WriteResponse(w, http.StatusOK, value)
	} else {
		h.WriteResponse(w, http.StatusNotFound, "Value Not found")
	}
}

// Save key-value pair
func (s *Server) SetRequest(w http.ResponseWriter, r *http.Request) {
	// Validate HTTP method
	if r.Method != "GET" {
		h.WriteResponse(w, http.StatusMethodNotAllowed, "Invalid HTTP Method")
		return
	}

	// Extract Query Parameters
	KeyQuery := r.URL.Query()["key"]
	ValueQuery := r.URL.Query()["value"]
	if len(KeyQuery) == 0 {
		h.WriteResponse(w, http.StatusNotFound, "Key not found")
		return
	} else if len(ValueQuery) == 0 {
		h.WriteResponse(w, http.StatusNotFound, "Value not found")
		return
	}

	// Extract Key and Value
	key := KeyQuery[0]
	value := ValueQuery[0]
	if len(key) > 50 {
		h.WriteResponse(w, http.StatusBadRequest, "Key length too long")
		return
	} else if len(value) > 100 {
		h.WriteResponse(w, http.StatusBadRequest, "Value length too long")
		return
	}

	// Save key-value to storage
	_, err := s.log.UpdateLog("SET", key, value)

	if err != nil {
		log.Println("Error writing to log - ", err)
		h.WriteResponse(w, http.StatusInternalServerError, "Internal Server Error")
		return
	}

	s.mp.SetValue(key, value)
	h.WriteResponse(w, http.StatusOK, "Key saved")

	// // Propagate change to other nodes
	// err = network.PropagateChange(newLog)
	// if err != nil {
	// 	log.Println("Could not propagate change to other nodes - ", err)
	// 	return err
	// }
}

// Delete key-value pair
func (s *Server) DeleteRequest(w http.ResponseWriter, r *http.Request) {
	// Validate HTTP method
	if r.Method != "GET" {
		h.WriteResponse(w, http.StatusMethodNotAllowed, "Invalid HTTP Method")
		return
	}

	// Extract Query Paramter
	KeyQuery := r.URL.Query()["key"]
	if len(KeyQuery) == 0 {
		h.WriteResponse(w, http.StatusNotFound, "Key not found")
		return
	}

	// Extract key
	key := KeyQuery[0]

	// Delete key-value from storage
	_, err := s.log.UpdateLog("DELETE", key, "")

	if err != nil {
		log.Println("Error writing to log - ", err)
		h.WriteResponse(w, http.StatusInternalServerError, "Internal Server Error")
		return
	}

	s.mp.DeleteValue(key)
	h.WriteResponse(w, http.StatusOK, "Key deleted")
}

// Recieve and mark WAL updates from other nodes
func InternalUpdateRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		h.WriteResponse(w, http.StatusMethodNotAllowed, "Invalid HTTP Method")
		return
	}

	// Read request body
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println("Could not read POST body - ", err)
		h.WriteResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	var newLog map[string]string
	err = json.Unmarshal(b, &newLog)
	if err != nil {
		log.Println("Error unmarshaling POST body - ", err)
		h.WriteResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Open log file
	// USE AN EXPORTED FUNCTION HERE
	// file, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	// if err != nil {
	// 	log.Println("Could not write to WAL log - ", err)
	// 	h.WriteResponse(w, 400, "Internal Server Error")
	// 	return
	// }
	// defer file.Close()

	// // Write to log file
	// _, err = file.WriteString(newLog["update"] + "\n")
	// if err != nil {
	// 	log.Println("Could not write to WAL log - ", err)
	// 	h.WriteResponse(w, 400, "Internal Server Error")
	// }

	// Update In-memory map
	// USE AN EXPORTED FUNCTION HERE
	// line := strings.Split(newLog["update"], ",")

	// if line[1] == "SET" {
	// 	storage.SetValue(line[2], line[3])
	// } else if line[2] == "DELETE" {
	// 	storage.DeleteValue(line[2])

	// }
	h.WriteResponse(w, http.StatusOK, "OK")
}
