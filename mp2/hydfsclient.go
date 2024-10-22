package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func commandListener() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input: ", err)
			continue
		}
		args := strings.Fields(input)
		if len(args) < 1 {
			fmt.Println("No command entered")
			continue
		}
		command := args[0]
		args = args[1:] // Extract the arguments after the command
		switch command {
		case "list_mem":
			listMembership()
		case "list_self":
			listSelf()
		case "join":
			joinGroup()
		case "leave":
			leaveGroup()
		case "enable_sus":
			enableSuspicion()
		case "disable_sus":
			disableSuspicion()
		case "list_sus":
			listSuspectedNodes()
		case "status_sus":
			statusSuspicion()
		case "create":
			if len(args) < 2 {
				fmt.Println("Error: Insufficient arguments. Usage: create localfilename HyDFSfilename")
				continue
			}
			localFilename := args[0]
			hyDFSFilename := args[1]
			fileContent := readFileToMessageBuffer(localFilename, hyDFSFilename)
			//@TODO pick targets here
			sendMessageViaTCP(introducerIP, fmt.Sprintf("%s,%s", "CREATE", fileContent))
		case "list_successors":
			printSuccessors()
		case "routing_table":
			printRoutingTable()
		default:
			fmt.Println("Unknown command")
		}
	}
}
