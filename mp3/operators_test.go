package main

import (
  "testing"
)

var line = "This is a test line that will be split, This is an example line that is under test"
var rt = Rainstorm_tuple_t{Key: "test_source.txt:0", Value: line}

func TestSplitLine(t *testing.T) {
	result := splitLine(rt)
	expected := []Rainstorm_tuple_t{
    {Key: "This", Value: "1"},
		{Key: "is", Value: "1"},
		{Key: "a", Value: "1"},
		{Key: "test", Value: "1"},
		{Key: "line", Value: "1"},
		{Key: "that", Value: "1"},
		{Key: "will", Value: "1"},
		{Key: "be", Value: "1"},
		{Key: "split", Value: "1"},
    {Key: ",", Value: "1"},
		{Key: "This", Value: "1"},
		{Key: "is", Value: "1"},
		{Key: "an", Value: "1"},
		{Key: "example", Value: "1"},
		{Key: "line", Value: "1"},
		{Key: "that", Value: "1"},
		{Key: "is", Value: "1"},
		{Key: "under", Value: "1"},
		{Key: "test", Value: "1"},
	}
	if len(result) != len(result) {
		t.Fatalf("Expected %d tuples, got %d", len(expected), len(result))
	}
	for i, tuple := range expected {
		if tuple.Key != expected[i].Key || tuple.Value != expected[i].Value {
			t.Errorf("Mismatch at index %d: expected %v, got %v", i, expected[i], tuple)
		}
	}
}

func TestCountOccureneces(t *testing.T) {
  result := splitLine(rt)
  for _, tuple := range result {
    wordCountOperator(tuple)
  }
}
