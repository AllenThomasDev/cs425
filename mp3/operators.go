package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type OperatorFunc func(data interface{}) interface{}

type StatelessArgs struct {
	rt Rainstorm_tuple_t
}

type StatefulArgs struct {
	rt Rainstorm_tuple_t
	port string
}

type Operator struct {
	Name     string
	Operator OperatorFunc
	Stateful bool
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
	Hash		int
	UIDBuf		*[]string // buffer of UIDs we're currently processing (either in input channel or outputting)
	UIDBufLock  *sync.Mutex
}

var operators = make(map[string]Operator)
var portToOpData = make(map[string]OperatorData)

func updateWordCount(key string, port string) {
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

func initOperators() {
	operators["source"] = Operator{
		Name: "source",
		Operator: func(data interface{}) interface{} {
      rainstormLog.Printf("I am a source\n")
			rt := data.(StatelessArgs).rt
			fileInfo := rt.Key
			fileInfoParts := strings.Split(fileInfo, ":")
			fileName := fileInfoParts[0]
      startLine, _ := strconv.Atoi(fileInfoParts[1])
      startChar, _ := strconv.Atoi(fileInfoParts[2])
      numLines, _ := strconv.Atoi(fileInfoParts[3])
      tupleChannel := make(chan Rainstorm_tuple_t)
      go generateSourceTuples(fileName, startLine, startChar, numLines, tupleChannel)
      return tupleChannel
		},
		Stateful: false,
	}

	operators["splitLineOperator"] = Operator{
		Name: "splitLineOperator",
		Operator: func(data interface{}) interface{} {
			rainstormLog.Printf("I am splitting\n")
			rt := data.(StatelessArgs).rt
			words := strings.Fields(rt.Value)
			tupleChannel := make(chan Rainstorm_tuple_t)
			go func() {
				for _, word := range words {
					tupleChannel <- Rainstorm_tuple_t{
            			Key: word,
						Value: "",
					}
				}

				close(tupleChannel) // Close the channel once all tuples are sent
			}() // <-- Invoke the anonymous function here
			return tupleChannel // Return the channel
		},
		Stateful: false,
	}

	operators["wordCountOperator"] = Operator{
		Name: "wordCountOperator",
		Operator: func(data interface{}) interface{} {
			rainstormLog.Printf("I am counting words\n")
			rt := data.(StatefulArgs).rt
			port := data.(StatefulArgs).port
      		updateWordCount(rt.Key, port)
			return Rainstorm_tuple_t{
				Key:   rt.Key,
				Value: portToOpData[port].StateMap[rt.Key],
			}
		},
		Stateful: true,
	}
	fmt.Println("Available Operators are - ")
	for key := range operators {
		fmt.Println(key)
	}
}

func validateOperations(operations []string) bool {
	for _, op := range operations {
		if _, exists := operators[op]; !exists {
			fmt.Printf("Error - Operation %s does not exist \nrefer to the legal operations that are registered \n\n", op)
			return false
		}
	}
	return true
}
