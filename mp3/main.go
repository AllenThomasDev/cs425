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
	
	file1, err := os.OpenFile("rainstorm.log", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open logfile1: %v", err)
	}
	defer file1.Close()
	rainstormLog = log.New(file1, "RS: ", log.Lmicroseconds)
	deleteFilesOnServer()
	err = os.MkdirAll("./server", os.ModePerm)
	deleteLocalLogs()
	err = os.MkdirAll("./client/local_logs", os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create ./server directory: %v", err)
	}
	go daemonMain()
	go commandListener()
  initOperators()
	go startRPCListenerHyDFS()
	// go periodicMerge()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	logger.Printf("%s was actually terminated at %d", selfIP, time.Now().Unix())
	fmt.Println("Shutting down daemon...")
}
