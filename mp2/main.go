package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	err := os.MkdirAll("./client", os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create ./client directory: %v", err)
	}
	deleteFilesOnServer()
	err = os.MkdirAll("./server", os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create ./server directory: %v", err)
	}
	go daemonMain()
	go commandListener()
	go startRPCListener()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	logger.Printf("%s was actually terminated at %d", selfIP, time.Now().Unix())
	fmt.Println("Shutting down daemon...")
}
