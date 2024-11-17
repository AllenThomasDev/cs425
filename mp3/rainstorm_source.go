package main

import (
	"fmt"
	"io"
	"os"
)

func source_wrapper(hydfs_src_file string, log_file string, startLine int, startCharacter int, numLines int) {
	err := backgroundCommand(fmt.Sprintf("get %s %s", hydfs_src_file, hydfs_src_file))
	if err != nil {
		fmt.Printf("Error getting source file: %v\n", err)
		return
	}
	
	remainingLines := numLines
	src_file, err := os.OpenFile("client/" + hydfs_src_file, os.O_RDONLY, 0644)
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

		remainingLines--
		
		fmt.Printf("Sending line %s\n", line)
		fmt.Printf("ACK received!\n")
		
		uniqueID := startLine + numLines - remainingLines
		backgroundCommand(fmt.Sprintf("appendstring %d %s", uniqueID, log_file))
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