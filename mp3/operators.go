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

var operators = make(map[string]Operator)

var wordCounts = make(map[string]int)

func initOperators() {
	operators["splitLineOperator"] = Operator{
		Name: "splitLineOperator",
		Operator: func(rt Rainstorm_tuple_t) interface{} {
			words := strings.Fields(rt.Value)
			var tuples []Rainstorm_tuple_t
			for _, word := range words {
				tuples = append(tuples, Rainstorm_tuple_t{
					Key:   word,
					Value: strconv.Itoa(1),
				})
			}
			return tuples
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
  for key := range operators { fmt.Println(key) }
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
