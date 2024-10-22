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

func writeFile(fileName string, fileContent string) error {
	// Open the file with O_CREATE and O_EXCL flags to ensure it fails if the file already exists
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("file %s already exists", fileName)
		}
		return fmt.Errorf("error creating file %s: %v", fileName, err)
	}
	defer file.Close()

	// Write the file content
	_, err = file.WriteString(fileContent)
	if err != nil {
		return fmt.Errorf("error writing to file %s: %v", fileName, err)
	}

	return nil
}

func appendFile(fileName string, fileContent string) error {
	// Open the file with O_APPEND and O_WRONLY flags, ensuring it fails if the file does not exist
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file %s does not exist", fileName)
		}
		return fmt.Errorf("error opening file %s: %v", fileName, err)
	}
	defer file.Close()

	// Append the file content
	_, err = file.WriteString(fileContent)
	if err != nil {
		return fmt.Errorf("error appending to file %s: %v", fileName, err)
	}

	return nil
}

func readFileToString(localFileName string) (string, error) {
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
	message := string(fileBytes)
	return message, nil
}

func readFileToMessageBuffer(localFileName string) string {
	if !checkFileExists(localFileName) || !checkFileOpens(localFileName) {
		return ""
	}
	message, err := readFileToString(localFileName)
	if err != nil {
		fmt.Printf("Error reading file %s into buffer: %v\n", localFileName, err)
		return ""
	}
	return message
}
