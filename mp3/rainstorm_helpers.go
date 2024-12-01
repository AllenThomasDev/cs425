package main

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var UIDLock sync.Mutex

var isProcessed = func(uniqueID int, oldLogFile string) bool {
	// Lock the entire operation to ensure atomic check and update
	UIDLock.Lock()
	defer UIDLock.Unlock()

	// First, check the cache
	if _, exists := UIDCache[uniqueID]; exists {
		return true
	}

	// If not in cache, check the persistent log
	status, err := checkDuplicate(strconv.Itoa(uniqueID), oldLogFile)
	if err != nil {
		fmt.Printf("Error checking for duplicate: %v\n", err)
		return false
	}
	return status
}

// using the client copy is NOT fine here since we need a record that persists even if this node fails
// Log the fact that a record has been processed
var logProcessed = func(uniqueID int, logFile string) {
	err := backgroundCommand(fmt.Sprintf("appendstring %s %s", strconv.Itoa(uniqueID)+"\n", logFile))
	if err != nil {
		fmt.Printf("Error logging processed line %d: %v\n", uniqueID, err)
		return
	}

	// Update the cache
	UIDCache[uniqueID] = true
}

// using the client copy is fine here since duplicates will only come from logged changes at the time of repartitioning
func checkDuplicate(uniqueID string, oldLogFile string) (bool, error) {
	log, err := os.OpenFile("client/"+oldLogFile, os.O_RDONLY, 0644)
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

var processRecord = func(uniqueID int, line string, hydfsSrcFile string, logFile string, oldLogFile string, port string) {
	if isProcessed(uniqueID, oldLogFile) {
		fmt.Printf("Record with uniqueID %d has already been processed. Skipping.\n", uniqueID)
		return
	}
	fmt.Printf("Sending record with uniqueID %d %s\n", uniqueID, line)

	key := hydfsSrcFile + ":" + strconv.Itoa(uniqueID)
	args := ArgsWithSender{Rainstorm_tuple_t{key, line}, currentVM, port}

	// retry sends until we get through
	err := sendToNextStage(args)
	for {
		if err == nil {
			break
		}
		time.Sleep(time.Second)
		err = sendToNextStage(args)
	}

	logProcessed(uniqueID, logFile)
}

// sendToNextStage sends the tuple to the next stage via RPC or other means
func sendToNextStage(args ArgsWithSender) error {
	fmt.Printf("Sending tuple: %v\n", args.Rt)
	reply := getNextStageArgsFromScheduler(&args)
	fmt.Println(reply)
	replyParts := strings.Split(reply, ":")
	nextVM, err := strconv.Atoi(replyParts[0])
	if err != nil {
		fmt.Println("this is where i die")
	}
	nextPort := replyParts[1]
	fmt.Printf("Sending data to node %d, port %s\n", nextVM, nextPort)
	sendRequestToServer(nextVM, nextPort, &args)
	return nil
}

// generateTuple creates a key-value tuple
func generateTuple(key string, value string) Rainstorm_tuple_t {
	return Rainstorm_tuple_t{key, value}
}

func sendRequestToServer(vm int, port string, args *ArgsWithSender) {
	// Establish a connection to the RPC server
	client, err := rpc.Dial("tcp", vmToIP(vm)+":"+port)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer client.Close()
	var reply string
	err = client.Call("WorkerReq.HandleTuple", args, &reply)
	if err != nil {
		fmt.Println("Error during RPC call:", err)
		return
	}
	fmt.Println("Server reply:", reply)
}

func getNextStageArgsFromScheduler(args *ArgsWithSender) string {
	// Establish a connection to the RPC server
	fmt.Println("Asking scheduler where to send this")
	client, err := rpc.Dial("tcp", vmToIP(LEADER_ID)+":"+SCHEDULER_PORT)
	if err != nil {
		fmt.Println("Error connecting to scheduler:", err)
		return "ohhhhhhhhhh is it failing here?"
	}
	defer client.Close()
	var reply string
	fmt.Println("I reached before rpc call")
	err = client.Call("SchedulerReq.GetNextStage", args, &reply)
	if err != nil {
		fmt.Println("Error during RPC call:", err)
		return ""
	}
	fmt.Println("Server reply:", reply)
	return reply
}

func showTopology() {
	for i := 0; i < RAINSTORM_LAYERS; i++ {
		fmt.Printf("LAYER %d: ", i)
		for j := 0; j < len(topologyArray[i]); j++ {
			fmt.Printf("%d: %s |", topologyArray[i][j].VM, topologyArray[i][j].port)
		}
		fmt.Printf("\n")
	}
}

func convertFileInfoStructListToTuples(hydfsSrcFile string, input FileChunkInfo, numSources int) []Rainstorm_tuple_t {
	var tuples []Rainstorm_tuple_t
	for i := 0; i < numSources; i++ {
		rainstormTuple := Rainstorm_tuple_t{
			Key:   hydfsSrcFile + ":" + strconv.Itoa(input.StartLines[i]) + ":" + strconv.Itoa(input.StartChars[i]) + ":" + strconv.Itoa(input.LinesPerSource[i]),
			Value: "1",
		}
		tuples = append(tuples, rainstormTuple)
	}
	return tuples
}
