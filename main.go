package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/epod1121/Log-Aggregator/.gitignore/pb"
	"google.golang.org/protobuf/proto"
)

var offsetByteMap = make(map[int]int64)

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
func send(from string, time string, topic string, message string) {

	// take the time, topic, and message and turn them into protobuf
	log := &pb.Log {
		From:		from,
		Time:		time,
		Topic:		topic,
		Message:	message,
	}

	passTopic := topic

	// marshal (turn into bytes) the log data
	data, err := proto.Marshal(log)
	if err != nil {
		fmt.Println("Error marshalling")
	}

	acceptLog(passTopic, data)
	// send those thru tcp connection from newLogProducer()
}

// Broker - Centralized server that accepts logs and organizes them
// into topics then persists them to disk

// listens for incoming producers and consumers
func startServer() {

	// open localhost port
	err := http.ListenAndServe(":8001", nil)
	if err != nil {
		fmt.Println("Port failed to open")
	}
	
	// run a loop that listens for connections
	for {

	}

	// when a connection comes in, send to handleConnection()
}

// determines if incoming connection is producer or consumer
func handleConnection() {

	// read first bytes of network
	// handle if producer or consumer
}

// coordinate storing the message safely
func acceptLog(topic string, message []byte) {

	nextByte := int64(0)
	offset := len(offsetByteMap)

	// open folder / file for specific topic
	topic = "placeholder"
	filename := fmt.Sprintf("Logs/%s.log", topic)
	file, err := os.OpenFile(filename, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error opening file")
		return
	}
	defer file.Close()

	// check current size of the file for byte position
	fileSize, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file stat")
		return
	}

	nextByte = fileSize.Size()

	// save key value pair [offset]byte
	offsetByteMap[offset] = nextByte
	offset++

	// call persisLog() to save to disk
	persistLog(file, message)
}

// write the raw data to drive
func persistLog(file *os.File, data []byte) {

	// write the bytes to the file
	success, err := file.Write(data)
	if err != nil {
		fmt.Println("Error persisting data")
		return
	}
	defer file.Close()

	fmt.Printf("Wrote %v bytes to disk\n", success)
	// call file.Sync()
}

// Consumer - Applications that read logs from the broker (sequentially)
// and process them

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
