package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

func waitForAck() ack_type_t {
	select {
	case ack := <-ackChannel:
		return ack
	case <-time.After(HYDFS_TIMEOUT):
		return TIMEOUT_ACK
	}
}

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

func writeFile(fileName string, fileContent string, writeTo string) error {
	// Open the file with O_CREATE and O_EXCL flags to ensure it fails if the file already exists
	file, err := os.OpenFile(writeTo + "/" + fileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			fmt.Printf("File already exists\n")
			return err
		}
		return fmt.Errorf("error creating file %s: %v", fileName, err)
	}

	// Write the file content
	_, err = file.WriteString(fileContent)
	if err != nil {
		return fmt.Errorf("error writing to file %s: %v", fileName, err)
	}

	return nil
}

func appendFile(fileName string, fileContent string) (string, error) {
	exists := checkFileExists("server/" + fileName)
	if exists {
		randFileName := genRandomFileName()
		err := writeFile(randFileName, fileContent, "server")
		if err != nil {
			fmt.Printf("ERROR IN APPEND\n")
			return "", fmt.Errorf("Error appending file %s: %v\n", fileName, err)
		}
		return randFileName, nil
	} else {
		fmt.Printf("ERROR IN APPEND\n")
		return "", fmt.Errorf("Error: file does not exist\n")
	}
}

func readFileToString(localFileName string, writeFrom string) (string, error) {
	localFile, err := os.Open(writeFrom + "/" + localFileName)
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

func readFileToMessageBuffer(localFileName string, writeFrom string) (string, error) {
	message, err := readFileToString(localFileName, writeFrom)
	if err != nil {
		fmt.Printf("Error reading file %s into buffer: %v\n", localFileName, err)
		return "", err
	}
	return message, nil
}

func removeFiles(repFiles []string) {
	for i := 0; i < len(repFiles); i++ {
		fmt.Printf("Removing file %s\n", repFiles[i])
		err := os.Remove("server/" + repFiles[i])
		if err != nil {
			fmt.Printf("Error removing file %s: %v\n", repFiles[i], err)
			continue
		} else {
			for j := 0; j < len(fileLogs[repFiles[i]]); j++ {
				// REMINDER: aIDtoFile uses filename, append id to give us randomized filename
				err = os.Remove("server/" + aIDtoFile[repFiles[i]][fileLogs[repFiles[i]][j]])
				if err != nil {
					fmt.Printf("Error removing shard %s: %v\n", aIDtoFile[repFiles[i]][fileLogs[repFiles[i]][j]], err)
				}
			}
		}
		// close channel, which will subsequently remove all file bookkeeping information
		close(fileChannels[repFiles[i]])
	}
}
