package main

import (
	"fmt"
	"io"
	"net"
	"os"

	"github.com/epod1121/Log-Aggregator/.gitignore/pb"
	"google.golang.org/protobuf/proto"
)

var (
	offsetByteMap = make(map[int]int64)
	offset = len(offsetByteMap)
)

func main() {
	fmt.Print("Starting Program...")
}

// need some place for the logs to actually come from
// going to simulate a high traffic online store with alerts
// as well as fake analytics for purchases, sing ups, etc.

// ======================================================================================
// Producer - get log messages and send them to broker
// ======================================================================================

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

	// NEED TO FIX THIS OFFSET THING RIGHT HERE
	// NEED TO FIX THIS OFFSET THING RIGHT HERE
	// NEED TO FIX THIS OFFSET THING RIGHT HERE
	// NEED TO FIX THIS OFFSET THING RIGHT HERE
	// NEED TO FIX THIS OFFSET THING RIGHT HERE

	
	// send those thru tcp connection from newLogProducer()
}



// ======================================================================================
// Broker - Centralized server that accepts logs and organizes them
// into topics then persists them to disk
// ======================================================================================

// listens for incoming producers and consumers
func startServer() {

	// open tcp port
	ln, err := net.Listen("tcp", ":9092")
	if err != nil {
		fmt.Println("Port failed to open")
		return
	}

	fmt.Println("Broker is listening on port 9092...")
	
	// run a loop that listens for connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection")
			continue
		}

		// when a connection comes in, send to handleConnection()
		// in a go routine for speed and load handling
		go handleConnection(conn)
	}
}

// determines if incoming connection is producer or consumer
func handleConnection(conn net.Conn) {

	defer conn.Close()
	idBuffer := make([]byte, 1)

	_, err := conn.Read(idBuffer)
	if err != nil {
		fmt.Println("Failed to read id")
		return
	}

	connectionType := idBuffer[0]

	switch connectionType {
	case 1:
		acceptLog(conn)

	case 2:
		streamLogs(conn)

	default:
		fmt.Println("Unknown connection type")
	}

}

// coordinate storing the message safely
func acceptLog(conn net.Conn) {

	nextByte := int64(0)

	// open folder / file for specific topic
	fileTopic := topic
	filename := fmt.Sprintf("Logs/%s.log", fileTopic)
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
	persistLog(file, conn)
}

// write the raw data to drive
func persistLog(file *os.File, conn net.Conn) {

	// write the bytes to the file
	success, err := file.Write(data)
	if err != nil {
		fmt.Println("Error persisting data")
		return
	}

	fmt.Printf("Wrote %v bytes to disk\n", success)
	// call file.Sync()
}

// streams data from disk to consumer
func streamLogs(conn net.Conn) {

	startStreaming := offsetByteMap[startOffset]
	// open folder / file for streaming
	fileTopic := topic
	filename := fmt.Sprintf("Logs/%s.log", fileTopic)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file")
		return
	}
	defer file.Close()

	data, err := file.Seek(startStreaming, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking to offset")
		return
	}

	var messageLength int64 = 15
	buf := make([]byte, messageLength)

	_, err = conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading buffer")
	}

	// need to send data directly to consumer over tcp connection
	// here just as a placeholder
	conn.Write(data)
}



// ======================================================================================
// Consumer - Applications that read logs from the broker (sequentially)
// and process them
// ======================================================================================

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
