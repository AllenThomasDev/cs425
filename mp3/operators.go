package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type OperatorFunc func(rt Rainstorm_tuple_t) interface{}
type Operator struct {
	Name     string
	Operator OperatorFunc
	Stateful bool
}

type OperatorPort struct {
	OperatorName string
	Port         string
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

type TupleAndAckInfo struct {
	Tup Rainstorm_tuple_t
	AckInfo Ack_info_t
}

type Ack_info_t struct {
	UID string
	SenderNum int
	SenderPort string
}

type OperatorChannels struct {
	Input		chan InputInfo
	Output		chan OutputInfo
	RecvdAck	chan Ack_info_t
	SendAck		chan bool
}

var operators = make(map[string]Operator)
var portToChannels = make(map[string]OperatorChannels)
// maps port current task is running on to next ACK we need to send out
// var portToNextAck = make(map[string]Ack_info_t)
var wordCountUpdates = make(chan string, 100) // Buffered channel for updates
var wordCounts sync.Map


func startWordCountWorker() {
  go func() {
    for key := range wordCountUpdates {
	// search for word we want to update in HyDFS logFile
      value, _ := wordCounts.LoadOrStore(key, 0)
	// append updated value to HyDFS state file
      wordCounts.Store(key, value.(int)+1)
      fmt.Println("Updated wordCounts:", key, value.(int)+1)
    }
  }()
}

func updateWordCount(key string) {
    wordCountUpdates <- key // Push key to the channel for processing
}


func initOperators() {
  startWordCountWorker()
	operators["source"] = Operator{
		Name: "source",
		Operator: func(rt Rainstorm_tuple_t) interface{} {
      fmt.Printf("I am a source\n")
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
		Operator: func(rt Rainstorm_tuple_t) interface{} {
			fmt.Printf("I am splitting\n")
			words := strings.Fields(rt.Value)
			tupleChannel := make(chan Rainstorm_tuple_t)
			go func() {
				for i, word := range words {
					tupleChannel <- Rainstorm_tuple_t{
            Key:   rt.Key + ":" + strconv.Itoa(i), // unique id, i'th word of unique id line
						Value: word,
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
		Operator: func(rt Rainstorm_tuple_t) interface{} {
			fmt.Printf("I am counting words\n")
      updateWordCount(rt.Value)
			return Rainstorm_tuple_t{
				Key:   rt.Value,
        //TODO: temporary 0 for testin
				Value: "0",
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
