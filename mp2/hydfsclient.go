package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"
)

// returns true on success, false on failure
func sendCreateAppend(message string, hash int) ack_type_t {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()

	if len(successors) <= 3 {
		for i := 0; i < len(successors); i++ {
			err := sendMessageViaTCP(vmToIP(successors[i]), message)
			if err != nil {
				return TIMEOUT_ACK
			}
			
			ack := waitForAck()
			if ack == TIMEOUT_ACK || ack == ERROR_ACK {
				return ack
			}
		}
		return GOOD_ACK
	} else {
		routingTableMutex.RLock()
		defer routingTableMutex.RUnlock()

		_, baseIndex := searchSuccessors(hash)
		for i := baseIndex; i != (baseIndex + 3) % len(successors); i = (i+1) % len(successors){
			err := sendMessageViaTCP(vmToIP(successors[i]), message)
			if err != nil {
				return TIMEOUT_ACK
			}

			ack := waitForAck()
			if ack == TIMEOUT_ACK || ack == ERROR_ACK {
				return ack
			}
		}
		print(len(ackChannel))
		return GOOD_ACK
	}
}

// this is different from the above function in that we only need to receive one get,
// where with create append we want to make sure it goes through at all machines
func sendGet(message string, hash int) ack_type_t {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()

	var ack ack_type_t
	if len(successors) <= 3 {
		for i := 0; i < len(successors); i++ {
			err := sendMessageViaTCP(vmToIP(successors[i]), message)
			if err != nil {
				continue
			}

			ack = waitForAck()
			if ack == GOOD_ACK {
				return ack
			}
		}
		return ack
	} else {
		routingTableMutex.RLock()
		defer routingTableMutex.RUnlock()

		_, baseIndex := searchSuccessors(hash)
		for i := baseIndex; i != (baseIndex + 3) % len(successors); i = (i+1) % len(successors){
			err := sendMessageViaTCP(vmToIP(successors[i]), message)
			if err != nil {
				continue
			}
			
			ack = waitForAck()
			if ack == GOOD_ACK {
				return ack
			}
		}
		return ack
	}
}

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
			fileContent, err := readFileToMessageBuffer(localFilename, "client")
			if err != nil {
				fmt.Printf("Error reading file: %v\n", err)
			} else {
				ts := time.Now()
				for {
					ack := sendCreateAppend(fmt.Sprintf("APPEND,%s,%s,%s,%s,%d", hyDFSFilename, fileContent, selfIP, ts.String(), currentVM),
							routingTable[hash(hyDFSFilename)])
					
					if ack == ERROR_ACK {
						fmt.Printf("Error on operation: system in undetermined state!\n")
						if PANIC_ON_ERROR == 1 {
							panic(fmt.Errorf("System in undetermined state"))
						}
						break
					}
					if ack == GOOD_ACK {
						break
					}
				}
			}
		case "create":
			if len(args) < 2 {
				fmt.Println("Error: Insufficient arguments. Usage: create localfilename HyDFSfilename")
				continue
			}
			localFilename := args[0]
			hyDFSFilename := args[1]
			fileContent, err := readFileToMessageBuffer(localFilename, "client")
			if err != nil {
				fmt.Printf("Error reading file: %v\n", err)
			} else {
				for {
					ack := sendCreateAppend(fmt.Sprintf("CREATE,%s,%s,%s", hyDFSFilename, fileContent, selfIP), routingTable[hash(hyDFSFilename)])
					if ack == ERROR_ACK {
						fmt.Printf("Error on operation: system in undetermined state!\n")
						if PANIC_ON_ERROR == 1 {
							panic(fmt.Errorf("System in undetermined state"))
						}
						break
					}
					if ack == GOOD_ACK {
						break
					}
				}
			}
		case "get":
			if len(args) < 2 {
				fmt.Println("Error: Insufficient arguments. Usage: get  HyDFSfilename localfilename")
				continue
			}
			hyDFSFilename := args[0]
			localFilename := args[1]
			
			for {
				ack := sendGet(fmt.Sprintf("GET,%s,%s,%s", hyDFSFilename, localFilename, selfIP), routingTable[hash(hyDFSFilename)])
				if ack == ERROR_ACK {
					fmt.Printf("Error on operation: system in undetermined state!\n")
					if PANIC_ON_ERROR == 1 {
						panic(fmt.Errorf("System in undetermined state"))
					}
					break
				}
				if ack == GOOD_ACK {
					break
				}
			}
			fmt.Println("I sent a GET message")
		case "list_successors":
			printSuccessors()
		case "routing_table":
			printRoutingTable()
		default:
			fmt.Println("Unknown command")
		}
	}
}
