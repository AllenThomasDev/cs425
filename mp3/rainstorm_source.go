package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
)

// SourceWrapper processes the file chunk line by line and sends to the next stage
func sourceWrapper(hydfsSrcFile, logFile string, startLine, startChar, numLines int) {
	// Fetch and open the file chunk
	tempFileName := genRandomFileName()
	err := backgroundCommand(fmt.Sprintf("get %s %s", hydfsSrcFile, tempFileName))
	if err != nil {
		fmt.Printf("Error fetching source file: %v\n", err)
		return
	}
	defer os.Remove("client/" + tempFileName)

	// Open file for reading
	file, err := os.OpenFile("client/"+tempFileName, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening source file: %v\n", err)
		return
	}
	defer file.Close()

	// Seek to start character
	_, err = file.Seek(int64(startChar), io.SeekStart)
	if err != nil {
		fmt.Printf("Error seeking file: %v\n", err)
		return
	}

	// Fetch log file to check for duplicates
	tempLogFileName := genRandomFileName()
	err = backgroundCommand(fmt.Sprintf("get %s %s", logFile, tempLogFileName))
	if err != nil {
		fmt.Printf("Error fetching log file: %v\n", err)
		return
	}
	defer os.Remove("client/" + tempLogFileName)

	// Process the chunk
	remainingLines := numLines
	for remainingLines > 0 {
		line, err := readLineFromFile(file)
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF reached.")
				break
			}
			fmt.Printf("Error reading line: %v\n", err)
			return
		}

    // why is filename not being used?
		uniqueID := startLine + numLines - remainingLines
		processed, err := checkDuplicate(tempLogFileName, strconv.Itoa(uniqueID))
		if err != nil {
			fmt.Printf("Error checking duplicates: %v\n", err)
			return
		}

		if !processed {
			tuple := generateTuple(uniqueID, line)
			sendToNextStage(tuple)
			logProcessed(uniqueID, logFile)
		} else {
			fmt.Printf("Line %d already processed. Skipping.\n", uniqueID)
		}

		remainingLines--
	}
}

// readLineFromFile reads a single line from the file


// checkDuplicate verifies if a unique ID exists in the log file
func checkDuplicate(logFile, uniqueID string) (bool, error) {
	log, err := os.OpenFile("client/"+logFile, os.O_RDONLY, 0644)
	if err != nil {
		return false, err
	}
	defer log.Close()

	for {
		line, err := readLineFromFile(log)
		if err != nil {
			if err == io.EOF {
				return false, nil
			}
			return false, err
		}

		if line == uniqueID {
			return true, nil
		}
	}
}

// generateTuple creates a key-value tuple from the unique ID and line
func generateTuple(uniqueID int, line string) map[string]string {
	return map[string]string{
		"key":   strconv.Itoa(uniqueID),
		"value": line,
	}
}

// sendToNextStage sends the tuple to the next stage via RPC or other means
func sendToNextStage(tuple map[string]string) {
	fmt.Printf("Sending tuple: %v\n", tuple)
	// Implement RPC or network logic here
}

// logProcessed appends the unique ID to the log file
func logProcessed(uniqueID int, logFile string) {
	err := backgroundCommand(fmt.Sprintf("appendstring %d %s", uniqueID, logFile))
	if err != nil {
		fmt.Printf("Error logging processed line %d: %v\n", uniqueID, err)
	}
}


func readLineFromFile(f *os.File) (string, error) {
	var line string
	b := make([]byte, 1)
	for {
		_, err := f.Read(b)
		if err != nil {
			return "", err
		}

		if string(b) == "\n" {
			return line, nil
		}

		line += string(b)
	}
}


