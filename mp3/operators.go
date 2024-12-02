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

type OperatorChannels struct {
	Input  chan Rainstorm_tuple_t
	Output chan Rainstorm_tuple_t
}

var operators = make(map[string]Operator)
var portToChannels = make(map[string]OperatorChannels)
var wordCountUpdates = make(chan string, 100) // Buffered channel for updates
var wordCounts sync.Map


func startWordCountWorker() {
  go func() {
    for key := range wordCountUpdates {
      value, _ := wordCounts.LoadOrStore(key, 0)
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
      fmt.Printf("I am a source %s", rt.Key)
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
      updateWordCount(rt.Value)
			return Rainstorm_tuple_t{
				Key:   rt.Key,
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
