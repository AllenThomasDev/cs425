package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
)

func source_wrapper(hydfs_src_file string, log_file string, startLine int, startCharacter int, numLines int) {
	
	randomSrcFileName := genRandomFileName()
	err := backgroundCommand(fmt.Sprintf("get %s %s", hydfs_src_file, randomSrcFileName))
	if err != nil {
		fmt.Printf("Error getting source file: %v\n", err)
		return
	}
	
	randomLogFileName := genRandomFileName()
	err = backgroundCommand(fmt.Sprintf("get %s %s", log_file, randomLogFileName))
	if err != nil {
		fmt.Printf("Error getting log file: %v\n", err)
		return
	}
	
	remainingLines := numLines
	src_file, err := os.OpenFile("client/" + randomSrcFileName, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
	}
	src_file.Seek(int64(startCharacter), io.SeekStart)

	for {
		if remainingLines == 0 {
			break
		}
		
		line, err := readLineFromFile(src_file)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("EOF reached\n")
			} else {
				fmt.Printf("Error on reading file: %v\n", err)
			}
			break
		}

		uniqueID := startLine + numLines - remainingLines
		idInLog, err := searchLog(randomLogFileName, strconv.Itoa(uniqueID))
		if err != nil {
			fmt.Printf("Error on log search: %v\n")
			return
		}

		// if we haven't already processed this line, go ahead and process it
		if idInLog == false {
			fmt.Printf("Sending line %s\n", line)
			fmt.Printf("ACK received!\n")
			backgroundCommand(fmt.Sprintf("appendstring %d %s", uniqueID, log_file))
		} else {
			fmt.Printf("Unique ID %d already in log, continuing...\n", uniqueID)
		}

		remainingLines--
	}

	// remove background files
	os.Remove("client/" + randomSrcFileName)
	os.Remove("client/" + randomLogFileName)
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

func searchLog(log_file string, uniqueID string) (bool, error) {
	lf, err := os.OpenFile("client/" + log_file, os.O_RDONLY, 0644)
	if err != nil {
		return false, err
	}

	for {
		line, err := readLineFromFile(lf)
		if err != nil {
			if err == io.EOF {
				return false, nil
			} else {
				return false, err
			}
		}

		if uniqueID == line {
			return true, nil
		}
	}
}