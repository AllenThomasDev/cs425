package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
)

var UIDLock sync.Mutex

func isProcessed(uniqueID int) bool {
	// Lock the entire operation to ensure atomic check and update
	UIDLock.Lock()
	defer UIDLock.Unlock()

	// First, check the cache
	if _, exists := UIDCache[uniqueID]; exists {
		return true
	}

	// If not in cache, check the persistent log
	status, err := checkDuplicate(strconv.Itoa(uniqueID))
	if err != nil {
		fmt.Printf("Error checking for duplicate: %v\n", err)
		return false
	}

	// If not found in the log, log the record and update the cache
	if !status {
		logProcessed(uniqueID)
	}
	return status
}

// Log the fact that a record has been processed
func logProcessed(uniqueID int) {
	// Log the fact that a record has been processed
	err := backgroundCommand(fmt.Sprintf("appendstring %d %s", uniqueID, logFilePath))
	if err != nil {
		fmt.Printf("Error logging processed line %d: %v\n", uniqueID, err)
		return
	}

	// Update the cache
	UIDCache[uniqueID] = true
}

func checkDuplicate(uniqueID string) (bool, error) {
	log, err := os.OpenFile("client/"+logFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return false, err
	}
	defer log.Close()
	for {
		line, err := readLineFromFile(log)
		if err != nil {
			if err == io.EOF {
				return false, nil
			}
			return false, err
		}
		if line == uniqueID {
			return true, nil
		}
	}
}

func processRecord(uniqueID int, line string) {
	if isProcessed(uniqueID) {
		fmt.Printf("Record with uniqueID %d has already been processed. Skipping.\n", uniqueID)
		return
	}
	//@TODO: Process the record
	fmt.Printf("Processing record with uniqueID %d %s\n", uniqueID, line)
	// send line to next stage and after ack, log prcocessed
	logProcessed(uniqueID)
}
