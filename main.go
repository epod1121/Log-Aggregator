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

	// placeholder, will make it a random value
	price := 100

	fmt.Printf("A $%s purchase was made\n", price)
}

func paymentError() {

	// placeholder, will make it a random value
	price := 100

	fmt.Printf("A payment error occurred losing us $%s\n", price)
}

func addToCart() {

	item := "carrot"

	fmt.Printf("A customer added %s to their cart\n", item)
}

func newSignUp() {

	fmt.Println("A new customer just signed up")
}

func outOfStock() {

	item := "carrot"

	fmt.Printf("%s is out of stock!\n", item)
	restocking(item)
}

func restocking(item string) {

	fmt.Printf("Restocking %s...\n", item)
}

func criticalError() {
	
	fmt.Println("A critical error has occurred, page a worker!")
	wakeUpSleepDeprivedWorker()
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

	fmt.Println("Frantically drinking coffee and solving problem")
	// wait 3 seconds
	fmt.Println("Problem solved. Going back to sleep")
}