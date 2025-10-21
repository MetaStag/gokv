package helper

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

// Helper function for returning HTTP response
func WriteResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-type", "Application/json") // JSON response
	w.WriteHeader(statusCode)                          // Add HTTP status code

	// Format JSON and add it to response body
	resp := make(map[string]string)
	resp["message"] = message
	json.NewEncoder(w).Encode(resp)
}

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
