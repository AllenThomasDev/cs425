package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
)

func copyFile(sourcePath, destinationPath string) error {
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()
	destFile, err := os.Create(destinationPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destFile.Close()
	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file contents: %w", err)
	}
	err = destFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync destination file: %w", err)
	}
	return nil
}

func mockProcessRecord(uniqueID int, line string, hydfsSrcFile string, logFile string, oldLogFile string, port string) {
	if isProcessed(uniqueID, oldLogFile) {
		return
	}
	key := hydfsSrcFile + ":" + strconv.Itoa(uniqueID)
	args := GetNextStageArgs{Rainstorm_tuple_t{key, line}, currentVM, port}
	sendToNextStage(args)
  fmt.Printf("here hereh here\n")
	logProcessed(uniqueID, logFile)
}

func mockBackgroundCommand(command string) error {
	if command == "fail" {
		return fmt.Errorf("mock command failed")
	}
	return nil
}

func mockLogProcessed(uniqueID int, logFile string) {
	fmt.Printf("Logged uniqueID %d to file %s\n", uniqueID, logFile)
}

func mockRandomFileName() string {
  return "test_random_file_name"
}

func TestSourceWrapper(t *testing.T) {
	hydfsSrcFile := "test_source.txt"
	logFile := "test_log.txt"
	startLine := 0
	startChar := 0
	numLines := 1
	port := "8080"

  err := os.WriteFile("client/"+logFile, []byte(""), 0644) // Empty log file
	if err != nil {
		t.Fatalf("Failed to create test log file: %v", err)
	}
  // mock the get command by copying the source file to a temp
  copyFile("client/" + hydfsSrcFile, "client/"+mockRandomFileName())
  genRandomFileName = mockRandomFileName
  processRecord = mockProcessRecord
	backgroundCommand = mockBackgroundCommand
	logProcessed = mockLogProcessed
	sourceWrapper(hydfsSrcFile, logFile, startLine, startChar, numLines, port)
}
