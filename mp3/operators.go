package main

import (
	"strconv"
	"strings"
)

var wordCounts = make(map[string]int)

func splitLine(rt Rainstorm_tuple_t) []Rainstorm_tuple_t {
	words := strings.Fields(rt.Value)
	var tuples []Rainstorm_tuple_t
	for _, word := range words {
		tuples = append(tuples, Rainstorm_tuple_t{
			Key:   word,            // Append the index to the key to make each tuple unique
			Value: strconv.Itoa(1), // Assign each word to a new tuple
		})
	}
	return tuples
}

func wordCountOperator(rt Rainstorm_tuple_t) Rainstorm_tuple_t {
	wordCounts[rt.Key]++
  return Rainstorm_tuple_t{
		Key:   rt.Key,
		Value: strconv.Itoa(wordCounts[rt.Key]), // Running count as string
	}
}
