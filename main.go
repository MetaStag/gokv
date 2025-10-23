package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"gokv/api"
	"gokv/helper"
	"gokv/network"
	"gokv/storage"
)

func main() {
	// Check if all required files exist
	if !helper.ValidateFiles() {
		log.Println("Necessary files don't exist, Exiting")
		return
	}

	// Start database connection
	db, err := storage.InitDatabase()
	if err != nil {
		log.Println("Error initializing storage - ", err)
		return
	}
	defer db.Close()

	// Create In-memory map and load log file values
	mp := storage.InitMap()
	l, err := storage.InitLog()
	if err != nil {
		log.Println("Could not initialize WAL log - ", err)
		return
	}
	err = db.ScanDatabase(mp)
	if err != nil {
		log.Println("Could not scan database - ", err)
		return
	}

	// Update database every 5 seconds
	go func() {
		for {
			time.Sleep(time.Second * 5)
			err := db.UpdateDatabase(l)
			if err != nil {
				log.Println("Error saving to database - ", err)
				db.Close()
				os.Exit(1)
			}
		}
	}()

	// Connect to other nodes
	nodes, err := network.Init()
	if err != nil {
		log.Println("Could not connect to other nodes - ", err)
		return
	}

	// Periodically ping nodes to check if connection is alive
	go func() {
		for {
			time.Sleep(time.Minute * 2)
			if !nodes.Ping() {
				log.Println("Lost connection to other nodes, Exiting")
				db.Close()
				os.Exit(1)
			}
		}
	}()

	// Define port on which server will run
	PORT := ":8080"

	// Initialize API server
	srv := api.New(mp, l)

	// Define Routes
	http.HandleFunc("/ping", api.HealthCheck)
	http.HandleFunc("/internal/update", api.InternalUpdateRequest)
	http.HandleFunc("/get", srv.GetRequest)
	http.HandleFunc("/set", srv.SetRequest)
	http.HandleFunc("/delete", srv.DeleteRequest)

	// Start Server
	log.Printf("Server running on http://localhost%s\n", PORT)
	log.Panic(http.ListenAndServe(PORT, nil))
}
