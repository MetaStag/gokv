package network

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	h "key/helper"
	"key/storage"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
)

var (
	c     *http.Client // HTTP Client to talk to other nodes
	nodes []string     // List of connected nodes
	mutex sync.RWMutex // Manage access to shared resources
)

// Check health of node
func HealthCheck(w http.ResponseWriter, r *http.Request) {
	h.WriteResponse(w, 200, "OK")
}

// Run this function initially to connect to other nodes
// It finds the IP of other nodes from cluster.txt
func ConnectNodes() error {
	// Load data from cluster.txt
	data, err := os.ReadFile("cluster.txt")
	if err != nil {
		return err
	}

	// Find container name (node shouldnt connect to itself)
	cname := os.Getenv("CNAME")
	cname = "http://" + cname + ":8080"

	// Update nodes[]
	lines := bytes.Lines(data)
	for i := range lines {
		node := string(i)
		node = strings.TrimSpace(node) // remove newline from end of url
		if node == cname {             // so that node doesnt connect to itself
			continue
		}
		nodes = append(nodes, node)
	}

	// Ping nodes to check connection
	// Remove inactive clients from nodes[] list
	c = &http.Client{} // Initialize HTTP Client

	err = Ping()
	if err != nil {
		return err
	}

	// For debugging, remove later, replace above if statement with return err
	for _, v := range nodes {
		log.Println("Connected to node -", v)
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

// Propagate change to other nodes
func PropagateChange(newLog string) error {
	bodyFormat := fmt.Sprintf(`{"update": "%s"}`, newLog)

	for _, v := range nodes {
		body := strings.NewReader(bodyFormat)
		resp, err := c.Post(v+"/internal/update", "application/json", body)
		if err != nil {
			log.Println("Could not send changes to node - ", err)
			return err
		}
		resp.Body.Close()
	}
	return nil
}

// Recieve and mark WAL updates from other nodes
func InternalUpdateRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		h.WriteResponse(w, 400, "Invalid HTTP Method")
		return
	}

	// Read request body
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println("Could not read POST body - ", err)
		h.WriteResponse(w, 400, "Invalid request body")
		return
	}
	var newLog map[string]string
	err = json.Unmarshal(b, &newLog)
	if err != nil {
		log.Println("Error unmarshaling POST body - ", err)
		h.WriteResponse(w, 400, "Invalid request body")
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	// Open log file
	file, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		log.Println("Could not write to WAL log - ", err)
		h.WriteResponse(w, 400, "Internal Server Error")
		return
	}
	defer file.Close()

	// Write to log file
	_, err = file.WriteString(newLog["update"] + "\n")
	if err != nil {
		log.Println("Could not write to WAL log - ", err)
		h.WriteResponse(w, 400, "Internal Server Error")
	}

	// Update In-memory map
	line := strings.Split(newLog["update"], ",")

	if line[1] == "SET" {
		storage.SetValue(line[2], line[3])
	} else if line[2] == "DELETE" {
		storage.DeleteValue(line[2])

	}
	h.WriteResponse(w, 200, "OK")
}
