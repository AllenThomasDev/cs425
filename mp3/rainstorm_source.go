package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

// SourceWrapper processes the file chunk line by line and sends to the next stage
func generateSourceTuples(hydfsSrcFile, logFile string, startLine, startChar, numLines int, port string) []Rainstorm_tuple_t {
	// TODO: make this more robust. right now if source finishes operation before topologyArray is populated it causes issues
	time.Sleep(time.Second)

	// Fetch and open the file chunk
	tempFileName := genRandomFileName()
	err := backgroundCommand(fmt.Sprintf("get %s %s", hydfsSrcFile, tempFileName))
	if err != nil {
		fmt.Printf("Error fetching source file: %v\n", err)
		return nil
	}
	defer os.Remove("client/" + tempFileName)

	// Open file for reading
	file, err := os.OpenFile("client/"+tempFileName, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening source file: %v\n", err)
		return nil
	}
	defer file.Close()

	// Seek to start character
	_, err = file.Seek(int64(startChar), io.SeekStart)
	if err != nil {
		fmt.Printf("Error seeking file: %v\n", err)
		return nil
	}

	// Fetch log file to check for duplicates
	oldLogFile := genRandomFileName()
	err = backgroundCommand(fmt.Sprintf("get %s %s", logFile, oldLogFile))
	if err != nil {
		fmt.Printf("Error fetching log file: %v\n", err)
		return nil
	}
	defer os.Remove("client/" + oldLogFile)

	// Process the chunk
	var lines []Rainstorm_tuple_t
	remainingLines := numLines
	for remainingLines > 0 {
		line, err := readLineFromFile(file)
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF reached.")
				break
			}
			fmt.Printf("Error reading line: %v\n", err)
			return nil
		}
	  uniqueID := startLine + numLines - remainingLines
  	key := hydfsSrcFile + ":" + strconv.Itoa(uniqueID)
    lineTuple := generateTuple(key, line)
		lines = append(lines, lineTuple)
		remainingLines--
		if remainingLines == 0 {
			select {
			case _, ok := <-stopChannels[port]:
				if ok {
					fmt.Println("Received stop signal. Exiting...")
					return nil // Exit the function after processing the current chunk
				}
			default:
				// Continue if no stop signal is received
			}
		}
	}
	return lines
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
