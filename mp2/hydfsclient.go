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
		case "append":
			if len(args) < 2 {
				fmt.Println("Error: Insufficient arguments. Usage: append localfilename HyDFSfilename")
				continue
			}
			localFilename := args[0]
			hyDFSFilename := args[1]
			fileContent := readFileToMessageBuffer(localFilename)
			sendMessageViaTCP(introducerIP, fmt.Sprintf("APPEND,%s,%s", hyDFSFilename, fileContent))
		case "create":
			if len(args) < 2 {
				fmt.Println("Error: Insufficient arguments. Usage: create localfilename HyDFSfilename")
				continue
			}
			localFilename := args[0]
			hyDFSFilename := args[1]
			fileContent := readFileToMessageBuffer(localFilename)
			// remember tha tfile content is in the format "hyDFSFileName, fileContent"
			//@TODO pick targets here
			//targetIPs := []
			//for ips in targetIPs {
			//  sendMessageViaTCP(ip, fmt.Sprintf("CREATE,%s", fileContent))
			sendMessageViaTCP(introducerIP, fmt.Sprintf("CREATE,%s,%s", hyDFSFilename, fileContent))
		case "get":
			if len(args) < 2 {
				fmt.Println("Error: Insufficient arguments. Usage: get  HyDFSfilename localfilename")
				continue
			}
			hyDFSFilename := args[0]
			localFilename := args[1]
			sendMessageViaTCP(introducerIP, fmt.Sprintf("GET,%s,%s,%s", hyDFSFilename, localFilename, selfIP))
			fmt.Println("I sent a READ message")
		case "list_successors":
			printSuccessors()
		case "routing_table":
			printRoutingTable()
		default:
			fmt.Println("Unknown command")
		}
	}
}
