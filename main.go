package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	fmt.Print("Starting Program...")
}

// need some place for the logs to actually come from
// going to simulate a high traffic online store with alerts
// as well as fake analytics for purchases, sing ups, etc.

func payment() {

}

func paymentError() {

}

func addToCart() {
	
}

func newSignUp() {

}

func outOfStock() {

}

func restocking() {

}

func criticalError() {
	
}

// Producer - get log messages and send them to broker

func getLogMessage() {

}

// Broker - Centralized server that accepts logs and organizes them
// into topics then persists them to disk

func persistLog() {

}

// send each topic to a different log file to separate topics
func organizeLog(logFileName string) {

	fileName := logFileName
	filePath := fmt.Sprintf("Logs/%s", fileName)

	file, err := os.OpenFile(filePath, os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0644)
	if err != nil {
		fmt.Println(http.StatusInternalServerError)
	}
	defer file.Close()
}

func notifyConsumer() {

}

// Consumer - Applications that read logs from the broker (sequentially)
// and process them

// send all logs to a single file as a history file
// (in case this fake store gets into fake legal trouble)

func processLog() {

}

// Diagram of what it should look like
// [Producers]  ──(TCP/Protobuf)──>  [Log Broker]  ──(Appends to Disk)
//                                       │
//                                       └──(Streams)──> [Consumers]

// very necessary part of this program
func wakeUpSleepDeprivedWorker() {

}