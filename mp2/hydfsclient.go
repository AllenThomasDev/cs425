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
		}

		args := strings.Fields(input)
		if len(args) < 1 {
			fmt.Println("No command entered")
			continue
		}
		input = strings.TrimSpace(input)
		switch input {
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
			create(args)
		default:
			fmt.Println("Unknown command")
		}
	}
}
