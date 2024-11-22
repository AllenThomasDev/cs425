package main

import (
	"os"
	"testing"
)

func TestReadLineFromFile(t *testing.T) {
	testFilePath := "testfile.txt"
	testContent := "Line1\nLine2\nLine3\n"
	expectedLines := []string{"Line1", "Line2", "Line3"}
	err := os.WriteFile(testFilePath, []byte(testContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(testFilePath) // Clean up test file after test
	file, err := os.Open(testFilePath)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()
	for _, expectedLine := range expectedLines {
		line, err := readLineFromFile(file)
		if err != nil {
			t.Fatalf("Error while reading line: %v", err)
		}

		if line != expectedLine {
			t.Errorf("Expected '%s', got '%s'", expectedLine, line)
		}
	}
	_, err = readLineFromFile(file)
	if err == nil {
		t.Error("Expected EOF, but no error occurred")
	}
}
