package main

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

var UIDLock sync.Mutex

func isProcessed(uniqueID int, logFile string) bool {
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
	return status
}

// using the client copy is NOT fine here since we need a record that persists even if this node fails
// Log the fact that a record has been processed
func logProcessed(uniqueID int, logFile string) {
	err := backgroundCommand(fmt.Sprintf("appendstring %s %s", strconv.Itoa(uniqueID) + "\n", logFile))
	if err != nil {
		fmt.Printf("Error logging processed line %d: %v\n", uniqueID, err)
		return
	}

	// Update the cache
	UIDCache[uniqueID] = true
}

// using the client copy is fine here since duplicates will only come from logged changes at the time of repartitioning
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

func processRecord(uniqueID int, line string, hydfsSrcFile string, logFile string) {
	if isProcessed(uniqueID, logFile) {
		fmt.Printf("Record with uniqueID %d has already been processed. Skipping.\n", uniqueID)
		return
	}
	fmt.Printf("Sending record with uniqueID %d %s\n", uniqueID, line)
	key := hydfsSrcFile + ":" + strconv.Itoa(uniqueID)
	args := GetNextStageArgs{Rainstorm_tuple_t{key, line}, currentVM}
	client, err := rpc.DialHTTP("tcp", vmToIP(LEADER_ID) + ":" + SCHEDULER_PORT)
	if err != nil {
		fmt.Printf("Error dialing leader: %v\n", err)
	}

	var reply string
	err = client.Call("SchedulerReq.GetNextStage", args, &reply)
	if err != nil {
		fmt.Printf("Error getting next stage: %v\n", err)
	}
	
	nextVM, _ := strconv.Atoi(reply);
	fmt.Printf("Sending data to node %d\n", nextVM)
	// send line to next stage and after ack, log processed
	// if call to client fails, sleep for a second and try again to give the scheduler some time to update topology
	logProcessed(uniqueID, logFile)
}

// generateTuple creates a key-value tuple
func generateTuple(key string, value string) Rainstorm_tuple_t {
	return Rainstorm_tuple_t{key, value}
}
