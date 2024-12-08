package main

import (
	"encoding/csv"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

// type OperatorFunc func(data interface{}) interface{}

type StatelessArgs struct {
	rt Rainstorm_tuple_t
}

type StatefulArgs struct {
	rt Rainstorm_tuple_t
	port string
}

type FilterArgs struct {
	rt Rainstorm_tuple_t
	pattern string
}

// input needs both tuple and address/UID to ack so output knows who to ACK
type InputInfo struct {
	Tup Rainstorm_tuple_t
	AckInfo Ack_info_t
}

// output info needs tuple to send, UID to write to log, and address/UID to ACK
type OutputInfo struct {
	Tup Rainstorm_tuple_t
	AckInfo Ack_info_t
	UID string
}

type Ack_info_t struct {
	UID string
	SenderOp	string
	SenderHash	int
}

type OperatorData struct {
	Input		chan InputInfo
	Output		chan OutputInfo
	RecvdAck	chan string // channel of Acked UIDs we've received
	SendAck		chan bool // after we receive the next Ack, should we send out an Ack?
	Death		chan bool
	StateMap	map[string]string // so we don't have to go to statefile every time
	LogFile		string
	StateFile	string
	Op			string
	Exec		string
	OpType		Task_type_t
	Hash		int
	UIDBuf		*[]string // buffer of UIDs we're currently processing (either in input channel or outputting)
	UIDBufLock  *sync.Mutex
}

var portToOpData = make(map[string]OperatorData)
var OBJECTID_COLUMN = 2
var SIGNTYPE_COLUMN = 3
var SIGNPOST_COLUMN = 6
var CATEGORY_COLUMN = 8

func convertStringToRT(strRT string) Rainstorm_tuple_t {
	parts := strings.Split(strRT, "~")
	if len(parts) == 1 {
		return Rainstorm_tuple_t{parts[0], ""}
	}
	return Rainstorm_tuple_t{parts[0], parts[1]}
}

func convertRTToString(rt Rainstorm_tuple_t) string {
	return fmt.Sprintf("%s~%s", rt.Key, rt.Value)
}

func splitOnCommas(csvals string) []string {
	csvReader := csv.NewReader(strings.NewReader(csvals))
	rowVals, _ := csvReader.Read()
	rainstormLog.Println(rowVals)
	return rowVals
}

func updateState(key string, port string) {
	val, found := portToOpData[port].StateMap[key]
	if found {
		count, _ := strconv.Atoi(val)
		rainstormLog.Printf("Updating %s with count %d\n", key, count + 1)
		portToOpData[port].StateMap[key] = strconv.Itoa(count + 1)
	} else {
		rainstormLog.Printf("Updating %s with count %d\n", key, 1)
		portToOpData[port].StateMap[key] = strconv.Itoa(1)
	}
}

func filterLine(rt Rainstorm_tuple_t, pattern string) Rainstorm_tuple_t {
	if strings.Contains(rt.Value, pattern) {
		return rt
 	} else {
		return Rainstorm_tuple_t{FILTERED, FILTERED}
	}
}

func filterSignPost(rt Rainstorm_tuple_t, pattern string) Rainstorm_tuple_t {
	parts := splitOnCommas(rt.Value)
	if parts[SIGNPOST_COLUMN] == pattern {
		// return just the category so we can partition based on categories to keep state clean
		rainstormLog.Printf("Passing along %s:%s\n", rt.Key, rt.Value)
		return Rainstorm_tuple_t{parts[CATEGORY_COLUMN], ""}
	} else {
		rainstormLog.Printf("Filtered out %s:%s\n", rt.Key, rt.Value)
		return Rainstorm_tuple_t{FILTERED, FILTERED}
	}
}

// hardcoded columns for demo
func cutOutColumns(rt Rainstorm_tuple_t) Rainstorm_tuple_t {
	parts := splitOnCommas(rt.Value)
	return Rainstorm_tuple_t{parts[OBJECTID_COLUMN] + "," + parts[SIGNTYPE_COLUMN], ""}
}

func sourceOp(rt Rainstorm_tuple_t) chan Rainstorm_tuple_t {
	rainstormLog.Printf("I am a source\n")
	fileInfo := rt.Key
	fileInfoParts := strings.Split(fileInfo, ":")
	fileName := fileInfoParts[0]
	startLine, _ := strconv.Atoi(fileInfoParts[1])
	startChar, _ := strconv.Atoi(fileInfoParts[2])
	numLines, _ := strconv.Atoi(fileInfoParts[3])
	tupleChannel := make(chan Rainstorm_tuple_t)
	go generateSourceTuples(fileName, startLine, startChar, numLines, tupleChannel)
	return tupleChannel
}

func statelessOp(rt Rainstorm_tuple_t, ex string) Rainstorm_tuple_t {
	rainstormLog.Printf("Executing stateless op %s\n", ex)
	opOut, err := exec.Command("./" + ex, convertRTToString(rt)).Output()
	if err != nil {
		fmt.Printf("Error on statelessOp: %v\n", err)
		rainstormLog.Printf("opOut: %s\n", opOut)
		return Rainstorm_tuple_t{FILTERED, FILTERED}
	}
	return convertStringToRT(string(opOut))
}

func statefulOp(rt Rainstorm_tuple_t, ex string, port string) Rainstorm_tuple_t {
	rainstormLog.Printf("Executing stateful op %s\n", ex)
	var stateRT Rainstorm_tuple_t
	stateRT.Key = rt.Key
	
	val, found := portToOpData[port].StateMap[rt.Key]
	if found {
		stateRT.Value = val
	} else {
		stateRT.Value = strconv.Itoa(0)
		portToOpData[port].StateMap[rt.Key] = strconv.Itoa(1)
	}
	rainstormLog.Printf("Sending %s:%s to op\n", stateRT.Key, stateRT.Value)
	
	opOut, err := exec.Command("./" + ex, convertRTToString(stateRT)).Output()
	if err != nil {
		fmt.Printf("Error on statefulOp: %v\n", err)
		rainstormLog.Printf("opOut: %s\n", opOut)
		return Rainstorm_tuple_t{FILTERED, FILTERED}
	}

	updatedCount := convertStringToRT(string(opOut))
	portToOpData[port].StateMap[updatedCount.Key] = updatedCount.Value

	return updatedCount
}

func filterOp(rt Rainstorm_tuple_t, ex string, arg string) Rainstorm_tuple_t {
	rainstormLog.Printf("Executing filter op %s with arg %s\n", ex, arg)
	opOut, err := exec.Command("./" + ex, convertRTToString(rt), arg).Output()
	if err != nil {
		rainstormLog.Printf("Error on filterOp: %v\n", err)
		return Rainstorm_tuple_t{FILTERED, FILTERED}
	}
	rainstormLog.Printf("opOut: %s\n", opOut)
	return convertStringToRT(string(opOut))
}
