package main

import (
	"fmt"
	// "net/http"
	// "os"
	// "time"
)

func main() {
	fmt.Print("Starting Program...")
}

// need some place for the logs to actually come from
// going to simulate a high traffic online store with alerts
// as well as fake analytics for purchases, sing ups, etc.

// Producer - get log messages and send them to broker

// initialize connection to broker
func newLogProducer() {

	// return a structure that holds this connection
}

// package and ships a single log
func send() {

	// take the time, topic, and message
	// turn them into protobuf
	// send those thru tcp connection from newLogProducer()
}

// Broker - Centralized server that accepts logs and organizes them
// into topics then persists them to disk

// listens for incoming producers and consumers
func startServer() {

	// open localhost port
	// run a loop that listens for connections
	// when a connection comes in, send to handleConnection()
}

// determines if incoming connection is producer or consumer
func handleConnection() {

	// read first bytes of network
	// handle if producer or consumer
}

// coordinate storing the message safely
func acceptLog() {

	// open folder / file for specific topic
	// check current size of the file for byte position
	// determine offset by how many items are already stored in this topic
	// save key value pair [offset]byte
	// call persisLog() to save to disk
}

// write the raw data to drive
func persistLog() {

	// write the bytes to the file
	// call file.Sync()
}

// Consumer - Applications that read logs from the broker (sequentially)
// and process them

// send all logs to a single file as a history file
// (in case this fake store gets into fake legal trouble)

// request data from a specific point in time
func processLog() {

	// connect to broker and ask for logs
	// consumer receives binary
	// turn it back into readable text
	// update dashboard
}

// Diagram of what it should look like
// [Producers]  ──(TCP/Protobuf)──>  [Log Broker]  ──(Appends to Disk)
//                                       │
//                                       └──(Streams)──> [Consumers]
