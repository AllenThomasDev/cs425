package main

import (
	"testing"
)

func TestHash(t *testing.T) {
	// Define the input filename and expected output
	filename := "test.txt"
	expectedHash := 6 // SHA1 of "example.txt"
	actualHash := hash(filename)
	if actualHash != expectedHash {
		t.Errorf("hash(%q) = %q; want %q", filename, actualHash, expectedHash)
	}
}
