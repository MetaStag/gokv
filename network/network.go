package network

import (
	"bufio"
	"net/http"
	"os"
	"sync"
	"time"
)

// Network is a cluster of multiple nodes
type Network interface {
	Ping() bool // Occasionally ping other nodes to check connection
}

type nodes struct {
	client *http.Client // HTTP Client to ping other nodes
	nodes  []string     // list of connected nodes
	mutex  sync.RWMutex // Manage access to shared resource
}

// Create a network and connect to other nodes
// It finds the IP of other nodes from cluster.txt
func Init() (Network, error) {
	n := &nodes{
		client: &http.Client{Timeout: 5 * time.Second},
		nodes:  []string{},
		mutex:  sync.RWMutex{},
	}

	// Find container name (node shouldnt connect to itself)
	cname := os.Getenv("CNAME")
	if cname != "" {
		cname = "http://" + cname + ":8080"
	}

	// Read from cluster.txt and update nodes[]
	file, err := os.Open("cluster.txt")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		node := scanner.Text()
		if node == cname { // so that node doesnt connect to itself
			continue
		}
		n.nodes = append(n.nodes, node)
	}

	// Ping nodes to check connection
	// Remove inactive clients from nodes[] list
	// if !n.Ping() {
	// 	return nil, errors.New("no other nodes connected")
	// }

	return n, nil
}

// Ping other nodes to check if connection is alive
// Updates nodes[] if a connection breaks
// Return false if all connections fail, else return true
func (n *nodes) Ping() bool {
	// Copy nodes[] to temp[] to free resource quickly
	n.mutex.RLock()
	temp := make([]string, len(n.nodes))
	copy(temp, n.nodes)
	n.mutex.RUnlock()

	// Ping each node and save in newNodes[]
	var newNodes []string
	for _, v := range temp {
		resp, err := n.client.Get(v + "/ping")
		if err != nil || resp == nil {
			continue
		}

		if resp.StatusCode == http.StatusOK {
			newNodes = append(newNodes, v)
		}
		resp.Body.Close()
	}

	// If all pings failed, return false
	if len(newNodes) == 0 {
		return false
	}

	// Update nodes[]
	n.mutex.Lock()
	n.nodes = newNodes
	n.mutex.Unlock()
	return true
}

// // Propagate change to other nodes
// func PropagateChange(newLog string) error {
// 	bodyFormat := fmt.Sprintf(`{"update": "%s"}`, newLog)

// 	for _, v := range n.nodes {
// 		body := strings.NewReader(bodyFormat)
// 		resp, err := n.c.Post(v+"/internal/update", "application/json", body)
// 		if err != nil {
// 			log.Println("Could not send changes to node - ", err)
// 			return err
// 		}
// 		resp.Body.Close()
// 	}
// 	return nil
// }
