package main

import (
	"fmt"
	"strconv"
	"strings"
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
	Output chan interface{}
}

var operators = make(map[string]Operator)
var portToChannels = make(map[string]OperatorChannels)
var wordCounts = make(map[string]int)

func initOperators() {
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
				for _, word := range words {
					tupleChannel <- Rainstorm_tuple_t{
						Key:   word,
						Value: strconv.Itoa(1),
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
			wordCounts[rt.Key]++
			return Rainstorm_tuple_t{
				Key:   rt.Key,
				Value: strconv.Itoa(wordCounts[rt.Key]),
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
