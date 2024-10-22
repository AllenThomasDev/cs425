package main

import (
	"fmt"
	"io"
	"os"
)

func checkFileExists(localFileName string) bool {
	_, err := os.Stat(localFileName)
	if os.IsNotExist(err) {
		fmt.Printf("Error: Local file %s does not exist\n", localFileName)
		return false
	}
	return true
}

func checkFileOpens(localFileName string) bool {
	_, err := os.OpenFile(localFileName, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening local file\n")
		return false
	}
	return true
}

func readFileToString(localFileName string, hyDFSFileName string) (string, error) {
	localFile, err := os.Open(localFileName)
	if err != nil {
		return "", fmt.Errorf("error opening local file %s: %v", localFileName, err)
	}
	defer localFile.Close()

	// Read the file content into a buffer
	fileBytes, err := io.ReadAll(localFile)
	if err != nil {
		return "", fmt.Errorf("error reading file %s into buffer: %v", localFileName, err)
	}

	// Create a message in the format "hyDFSFileName, fileContent"
	message := hyDFSFileName + "," + string(fileBytes)
	return message, nil
}

func readFileToMessageBuffer(localFileName string, hyDFSFileName string) string {
	if !checkFileExists(localFileName) || !checkFileOpens(localFileName) {
		return ""
	}
	message, err := readFileToString(localFileName, hyDFSFileName)
	if err != nil {
		fmt.Printf("Error reading file %s into buffer: %v\n", localFileName, err)
		return ""
	}
	return message
}
