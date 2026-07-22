package main

import (
	"encoding/binary"
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

// open the actual connection
type Producer struct {
	conn net.Conn
}

// connect and hold open the connection to tcp address
func newLogProducer(address string) (*Producer, error){

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("Server is offline")
		return nil, err
	}
	return &Producer{conn: conn}, nil
}

// package and ships a single log
func (p *Producer) send(from string, time string, topic string, message string) error {

	// take the time, topic, and message and turn them into protobuf
	log := &pb.Log {
		From:		from,
		Time:		time,
		Topic:		topic,
		Message:	message,
	}

	// marshal (turn into bytes) the log data
	data, err := proto.Marshal(log)
	if err != nil {
		fmt.Println("Error marshalling")
		return err
	}

	// get lengths
    topicBytes := []byte(topic)
    topicLen := make([]byte, 4)
    binary.BigEndian.PutUint32(topicLen, uint32(len(topicBytes)))

    dataLen := make([]byte, 4)
    binary.BigEndian.PutUint32(dataLen, uint32(len(data)))

    // combine everything into a single network packet:
	// 1 byte id --> 4 byte topic len --> topic --> 4 byte data len --> data
	var packet []byte
    packet = append(packet, 1) // Secret knock (Producer)
    packet = append(packet, topicLen...)
    packet = append(packet, topicBytes...)
    packet = append(packet, dataLen...)
    packet = append(packet, data...)

    // send in one single TCP write
    _, err = p.conn.Write(packet)
    return err
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

	// read topic from conn
	topicLen, err := readLength(conn)
	if err != nil {
		fmt.Println("Error reading file length")
		return
	}
	// translates protobuf into the actual topic
	topicBuf := make([]byte, topicLen)
	_, err = io.ReadFull(conn, topicBuf)
	if err != nil {
		fmt.Println("Error reading topic")
		return
	}
	fileTopic := string(topicBuf)


	// read length of protobuf bytes
	dataLen, err := readLength(conn)
	if err != nil {
		fmt.Println("Error reading data length")
		return
	}
	// translates the actual protobuf into payload
	dataBuf := make([]byte, dataLen)
	_, err = io.ReadFull(conn, dataBuf)
	if err != nil {
		fmt.Println("Error reading data payload")
		return
	}

	
	// create "Logs" folder if it does not exist
	err = os.MkdirAll("Logs", 0755)
	if err != nil {
		fmt.Println("Error creating Log file")
		return
	}

	// open/create file for specific topic
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

	nextByte := fileSize.Size()

	// save key value pair [offset]byte
	offsetByteMap[offset] = nextByte
	offset++

	// send the file and the data to persist to it to save to disk
	persistLog(file, dataBuf)
}

// write the raw data to drive
func persistLog(file *os.File, data []byte) {

	// write the bytes to the file
	success, err := file.Write(data)
	if err != nil {
		fmt.Println("Error persisting data")
		return
	}

	fmt.Printf("Wrote %v bytes to disk\n", success)
	file.Sync()
}

// streams data from disk to consumer
func streamLogs(conn net.Conn) {

	// just like in acceptLog, get the file length and name from protobuf
	// read topic from conn
	topicLen, err := readLength(conn)
	if err != nil {
		fmt.Println("Error reading file length")
		return
	}
	// translates protobuf into the actual topic
	topicBuf := make([]byte, topicLen)
	_, err = io.ReadFull(conn, topicBuf)
	if err != nil {
		fmt.Println("Error reading topic")
		return
	}
	fileTopic := string(topicBuf)


	// read the offset value from conn
	startOffset, err := readOffset(conn)
	if err != nil {
		fmt.Println("Error reading start offset")
		return
	}
	targetByte := offsetByteMap[int(startOffset)]

	// open folder / file for streaming
	filename := fmt.Sprintf("Logs/%s.log", fileTopic)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file")
		return
	}
	defer file.Close()

	_, err = file.Seek(targetByte, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking to offset")
		return
	}

	// get the length of how long the requested streamed message is
	// while checking to see if the index is out of bounds
	var messageLength int64

	nextByte, exists := offsetByteMap[int(startOffset) + 1]
	if exists {
		messageLength = nextByte - targetByte
	} else {
		fileInfo, err := file.Stat()
		if err != nil {
			fmt.Println("Error getting file stat")
			return
		}
		messageLength = fileInfo.Size() - targetByte
	}

	buf := make([]byte, messageLength)

	_, err = file.Read(buf)
	if err != nil {
		fmt.Println("Error reading buffer")
	}

	// need to send data directly to consumer over tcp connection
	// here just as a placeholder
	_, err = conn.Write(buf)
	if err != nil {
		fmt.Println("Error streaming log")
		return
	}
}

// reads the length of the file when passed to acceptLog()
func readLength(conn net.Conn) (int32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		return 0, err
	}

	return int32(binary.BigEndian.Uint32(buf)), nil
}

// reads the offset of the file when reading from streamLogs()
func readOffset(conn net.Conn) (int64, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		return 0, nil
	}

	return int64(binary.BigEndian.Uint64(buf)), nil
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
