package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Define struct for data storage
type AudioFile struct {
	FileName   string `bson:"file_name" json:"file_name"`
	StorageURL string `bson:"storage_url" json:"storage_url"`
}

// Define global mongo client variable
var client *mongo.Client

// Mongo connection function, connects client to mongo atlast cluster
func connectionDB() *mongo.Client {
	// Connect client to the mongo atlas cluster
	clientOptions := options.Client().ApplyURI("mongodb+srv://walker005z:k3vX5kaH-C+V#3z@audio-cluster.zbc1o.mongodb.net/?retryWrites=true&w=majority&appName=audio-cluster")
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func getAudioFilesHandler(w http.ResponseWriter, r *http.Request) {
	// Establish connection to the collection
	collection := client.Database("AudioDB").Collection("AudioFiles")

	// Use cursor to query the collection
	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		http.Error(w, "Failed to retrieve audio files.", http.StatusInternalServerError)
		return
	}
	defer cursor.Close(context.TODO())

	// Channel to receive audio files
	// Concurrent Approach
	audioFilesChan := make(chan AudioFile)
	var wg sync.WaitGroup

	// Goroutine to decode audio files concurrently
	go func() {
		for cursor.Next(context.TODO()) {
			var audioFile AudioFile
			if err := cursor.Decode(&audioFile); err != nil {
				http.Error(w, "Error decoding audio file", http.StatusInternalServerError)
				return
			}
			wg.Add(1)
			go func(file AudioFile) {
				defer wg.Done()
				audioFilesChan <- file
			}(audioFile)
		}
		wg.Wait()
		close(audioFilesChan)
	}()

	// Collect audio files
	var audioFiles []AudioFile
	for file := range audioFilesChan {
		audioFiles = append(audioFiles, file)
	}

	// Empty array aka no files found, 404 error response
	if len(audioFiles) == 0 {
		http.Error(w, "No audio files found", http.StatusNotFound)
		return
	}

	/* non concurrent implimentation

	// Initialize audioFiles array
	var audioFiles []AudioFile

	// Parse cursors returned data
	for cursor.Next(context.TODO()) {
		//Initialize single audio file struct
		var audioFile AudioFile
		if err := cursor.Decode(&audioFile); err != nil {
			http.Error(w, "Error decoding audio file", http.StatusInternalServerError)
			return
		}
		audioFiles = append(audioFiles, audioFile)
	}

	// Empty array aka no files found, 404 error response
	if len(audioFiles) == 0 {
		http.Error(w, "No audio files found", http.StatusNotFound)
		return
	}

	*/

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(audioFiles)
}

func main() {
	// Conection to cluster
	client = connectionDB()

	// Set router & server
	http.HandleFunc("/api/audio-files", getAudioFilesHandler)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8092"
	}

	fmt.Printf("Server running on port %s...\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
	defer client.Disconnect(context.TODO())
}
