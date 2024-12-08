package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
)

// SourceWrapper processes the file chunk line by line and sends to the next stage
func generateSourceTuples(hydfsSrcFile string, startLine int, startChar int, numLines int, tupleChannel chan Rainstorm_tuple_t) {
	// Fetch and open the file chunk
	tempFileName := genRandomFileName()
	err := backgroundCommand(fmt.Sprintf("get %s %s", hydfsSrcFile, tempFileName))
	if err != nil {
		fmt.Printf("Error fetching source file: %v\n", err)
	}
	defer os.Remove("client/" + tempFileName)

	// Open file for reading
	file, err := os.OpenFile("client/"+tempFileName, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening source file: %v\n", err)
	}
	defer file.Close()

	// Seek to start character
	_, err = file.Seek(int64(startChar), io.SeekStart)
	if err != nil {
		fmt.Printf("Error seeking file: %v\n", err)
	}
	remainingLines := numLines
	for remainingLines > 0 {
		line, err := readLineFromFile(file)
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF reached.")
				break
			}
			fmt.Printf("Error reading line: %v\n", err)
		}
		uniqueID := startLine + numLines - remainingLines
		key := hydfsSrcFile + "," + strconv.Itoa(uniqueID)
		lineTuple := generateTuple(key, line)
    	tupleChannel <- lineTuple
		remainingLines--
	}
	close(tupleChannel)
}

// readLineFromFile reads a single line from the file
func readLineFromFile(f *os.File) (string, error) {
	line := ""
	b := make([]byte, 1)
	for {
		_, err := f.Read(b)
		if err != nil {
			if err == io.EOF && line != "" {
				return line, nil
			}
			return "", err
		}

		if string(b) == "\n" {
			return line, nil
		}

		line += string(b)
	}
}
