package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func userInput() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input: ", err)
		}

		args := strings.Fields(input)
		if len(args) < 1 {
			fmt.Println("No command entered")
			continue
		}
		
		command := args[0]
		args = args[1:]
		switch command {
		case "create":
			create(args)
		default:
			fmt.Println("Unknown command")
		}
	}
}
