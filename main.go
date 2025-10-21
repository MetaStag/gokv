package main

import (
	"log"
	"net/http"
	"time"

	"key/api"
	"key/helper"
	"key/network"
	"key/storage"
)

func main() {
	// Check if all required files exist
	if !helper.ValidateFiles() {
		log.Fatal("Necessary files don't exist, Exiting")
	}

	// Initialize storage (database + in-memory map)
	err := storage.InitStorage()
	if err != nil {
		log.Fatal("Error initializing storage - ", err)
	}
	defer storage.CloseStorage()

	// Update database every 5 seconds
	go func() {
		for {
			time.Sleep(time.Second * 5)
			err = storage.UpdateDatabase()
			if err != nil {
				log.Fatal("Error saving to database - ", err)
			}
		}
	}()

	// Connect to other nodes
	err = network.ConnectNodes()
	if err != nil {
		log.Fatal("Could not connect to other nodes - ", err)
	}

	// Periodically ping nodes to check if connection is alive
	go func() {
		for {
			time.Sleep(time.Minute * 2)
			log.Println("Running ping goroutine") // for debugging, remove later
			err := network.Ping()
			if err != nil {
				log.Fatal("Error pinging other nodes - ", err)
			}
		}
	}()

	// Define port on which server will run
	PORT := ":8080"

	// Define Routes
	http.HandleFunc("/ping", network.HealthCheck)
	http.HandleFunc("/get", api.GetRequest)
	http.HandleFunc("/set", api.SetRequest)
	http.HandleFunc("/delete", api.DeleteRequest)
	http.HandleFunc("/internal/update", network.InternalUpdateRequest)

	// Start Server
	log.Printf("Server running on http://localhost%s\n", PORT)
	log.Fatal(http.ListenAndServe(PORT, nil))
}
