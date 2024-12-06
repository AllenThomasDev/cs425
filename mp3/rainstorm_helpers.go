package main

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

// using the client copy is NOT fine here since we need a record that persists even if this node fails
// Log the fact that a record has been processed
var backgroundWrite = func(writeStr string, writeFile string) {
	rainstormLog.Printf("Trying to write %s to file %s\n", writeStr, writeFile)
	err := appendString(writeStr, writeFile)
	if err != nil {
		fmt.Printf("Error writing processed line %d: %v\n", writeStr, err)
		return
	}

	// Update the cache
	// UIDCache[uniqueID] = true
}

// using the client copy is fine here since duplicates will only come from logged changes at the time of repartitioning
func checkLogFile(uniqueID string, logFile string) (bool, error) {
	// fetch local copy of log file
	tempLogName := genRandomFileName()
	rainstormLog.Printf("Trying to get logfile %s with temp name %s\n", logFile, tempLogName)
	err := backgroundCommand(fmt.Sprintf("get %s %s", logFile, tempLogName))
	if err != nil {
		return false, err
	}

	rainstormLog.Printf("checking logfile for UID: %s\n", uniqueID)
	defer os.Remove("client/" + tempLogName)
	log, err := os.OpenFile("client/" + tempLogName, os.O_RDONLY, 0644)
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

// generateTuple creates a key-value tuple
func generateTuple(key string, value string) Rainstorm_tuple_t {
	return Rainstorm_tuple_t{key, value}
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

// sendToNextStage sends the tuple to the next stage via RPC or other means
func sendToNextStage(args GetNextStageArgs, UID string) error {
	reply := getNextStageArgsFromScheduler(args)
	replyParts := strings.Split(reply, ":")
	nextVM, err := strconv.Atoi(replyParts[0])
	if err != nil {
		fmt.Println("this is where i die")
	}
	nextPort := replyParts[1]
  nextStageArgs := ArgsWithSender{
    Rt: args.Rt,
	SenderOp: portToOpData[args.Port].Op,
    SenderHash: portToOpData[args.Port].Hash,
    TargetPort: nextPort,
	UID: UID}
	err = sendRequestToServer(nextVM, nextPort, &nextStageArgs)
	return err
}

func getNextStageArgsFromScheduler(args GetNextStageArgs) string {
	// Establish a connection to the RPC server
	client, err := rpc.Dial("tcp", vmToIP(LEADER_ID)+":"+SCHEDULER_PORT)
	if err != nil {
		rainstormLog.Println("Error connecting to scheduler:", err)
		return "ohhhhhhhhhh is it failing here?"
	}
	defer client.Close()
	var reply string
	err = client.Call("SchedulerReq.GetNextStage", &args, &reply)
	if err != nil {
		rainstormLog.Println("Error during RPC call:", err)
		return ""
	}
	return reply
}

func sendRequestToServer(vm int, port string, args *ArgsWithSender) error {
	// Establish a connection to the RPC server
	rainstormLog.Printf("sendRequest: Trying to send data to %d:%s\n", vm, port)
	client, err := rpc.Dial("tcp", vmToIP(vm)+":"+port)
	if err != nil {
		return err
	}
	defer client.Close()
	var reply string
	err = client.Call("WorkerReq.HandleTuple", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

func ackPrevStage(ack Ack_info_t) error {
	prevStageArgs := GetPrevStageArgs{
		PrevOperator: ack.SenderOp,
		PrevHash: ack.SenderHash,
	}

	reply := getPrevStageArgsFromScheduler(prevStageArgs)
	replyParts := strings.Split(reply, ":")
	prevVM, err := strconv.Atoi(replyParts[0])
	if err != nil {
		fmt.Println("this is where i die")
	}
	prevPort := replyParts[1]
  	ackArgs := ReceiveAckArgs{
		UID: ack.UID,
		Port: prevPort,
	}
    
	err = sendAckToServer(prevVM, prevPort, &ackArgs)
	return err
}

func getPrevStageArgsFromScheduler(args GetPrevStageArgs) string {
	// Establish a connection to the RPC server
	client, err := rpc.Dial("tcp", vmToIP(LEADER_ID)+":"+SCHEDULER_PORT)
	if err != nil {
		rainstormLog.Println("Error connecting to scheduler:", err)
		return "ohhhhhhhhhh is it failing here?"
	}
	defer client.Close()
	var reply string
	err = client.Call("SchedulerReq.GetPrevStage", &args, &reply)
	if err != nil {
		rainstormLog.Println("Error during RPC call:", err)
		return ""
	}
	return reply
}

func sendAckToServer(vm int, port string, args *ReceiveAckArgs) error {
	rainstormLog.Printf("Sending ACK with ID %s to %d:%s\n", args.UID, vm, port)
	client, err := rpc.Dial("tcp", vmToIP(vm) + ":" + port)
	if err != nil {
		return err
	}
	defer client.Close()
	var reply string
	err = client.Call("WorkerReq.ReceiveAck", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

func restoreState(stateFile string, port string) {
	err := processStateFile(stateFile, port)
	if err != nil {
		rainstormLog.Printf("Error restoring state: %v\n", err)
		return
	}

	rainstormLog.Printf("RESTORED STATE:\n")
	for key := range(portToOpData[port].StateMap) {
		rainstormLog.Printf("%s: %s\n", key, portToOpData[port].StateMap[key])
	}
}

func processStateFile(stateFile string, port string) error {
	tempFileName := genRandomFileName()
	rainstormLog.Printf("getting statefile %s\n", stateFile)
	err := backgroundCommand(fmt.Sprintf("get %s %s", stateFile, tempFileName))
	if err != nil {
		rainstormLog.Printf("Error fetching source file: %v\n", err)
	}
	defer os.Remove("client/" + tempFileName)

	sf, err := os.OpenFile("client/" + tempFileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer sf.Close()
	for {
		line, err := readLineFromFile(sf)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		lineParts := strings.Split(line, ":")
		key := lineParts[0]
		value := lineParts[1]
		portToOpData[port].StateMap[key] = value
	}
}
