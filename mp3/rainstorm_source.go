package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

// SourceWrapper processes the file chunk line by line and sends to the next stage
func sourceWrapper(hydfsSrcFile, logFile string, startLine, startChar, numLines int, port string) {
	// TODO: make this more robust. right now if source finishes operation before topologyArray is populated it causes issues
	time.Sleep(time.Second)
	
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
	oldLogFile := genRandomFileName()
	err = backgroundCommand(fmt.Sprintf("get %s %s", logFile, oldLogFile))
	if err != nil {
		fmt.Printf("Error fetching log file: %v\n", err)
		return
	}
	defer os.Remove("client/" + oldLogFile)

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
		uniqueID := startLine + numLines - remainingLines
		processRecord(uniqueID, line, hydfsSrcFile, logFile, oldLogFile, port)
		remainingLines--

		select {
		case _, ok := <-stopChannels[port]:
			if ok {
				return
			} else {
				fmt.Printf("Error: channel closed!\n")
				return
			}
		default:
		}
	}
}

// readLineFromFile reads a single line from the file
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
