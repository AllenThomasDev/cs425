package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"time"
)

func sendAppend(args AppendArgs, ip string) error {
	client, err := rpc.DialHTTP("tcp", ip+":"+RPC_PORT)
	if err != nil {
		return err
	}

	var reply string
	err = client.Call("HyDFSReq.Append", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

func sendAppendToQuorum(args AppendArgs, hash int) error {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()

	if len(successors) <= 3 {
		for i := 0; i < len(successors); i++ {
			err := sendAppend(args, vmToIP(successors[i]))
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		routingTableMutex.RLock()
		defer routingTableMutex.RUnlock()

		_, baseIndex := searchSuccessors(hash)
		for i := baseIndex; i != (baseIndex+3)%len(successors); i = (i + 1) % len(successors) {
			err := sendAppend(args, vmToIP(successors[i]))
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func sendCreate(args CreateArgs, ip string) error {
	client, err := rpc.DialHTTP("tcp", ip+":"+RPC_PORT)
	if err != nil {
		return err
	}

	var reply string
	err = client.Call("HyDFSReq.Create", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

func sendCreateToQuorum(args CreateArgs, hash int) error {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()

	if len(successors) <= 3 {
		for i := 0; i < len(successors); i++ {
			err := sendCreate(args, vmToIP(successors[i]))
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		routingTableMutex.RLock()
		defer routingTableMutex.RUnlock()

		_, baseIndex := searchSuccessors(hash)
		for i := baseIndex; i != (baseIndex+3)%len(successors); i = (i + 1) % len(successors) {
			err := sendCreate(args, vmToIP(successors[i]))
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func sendGet(args GetArgs, ip string) (string, error) {
	client, err := rpc.DialHTTP("tcp", ip+":"+RPC_PORT)
	if err != nil {
		return "", err
	}

	var reply string
	err = client.Call("HyDFSReq.Get", args, &reply)
	if err != nil {
		return "", err
	}

	return reply, nil
}

// this is different from the above function in that we only need to receive one get,
// where with create append we want to make sure it goes through at all machines
func sendGetToQuorum(args GetArgs, hash int) string {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()

	if len(successors) <= 3 {
		for i := 0; i < len(successors); i++ {
			reply, err := sendGet(args, vmToIP(successors[i]))
			if err != nil {
				continue
			}

			return reply
		}
		return ""
	} else {
		routingTableMutex.RLock()
		defer routingTableMutex.RUnlock()

		_, baseIndex := searchSuccessors(hash)
		for i := baseIndex; i != (baseIndex+3)%len(successors); i = (i + 1) % len(successors) {
			reply, err := sendGet(args, vmToIP(successors[i]))
			if err != nil {
				continue
			}

			return reply
		}
		return ""
	}
}

func sendMerge(args MergeArgs, hash int) error {
	routingTableMutex.RLock()
	defer routingTableMutex.RUnlock()

	client, err := rpc.DialHTTP("tcp", vmToIP(routingTable[hash])+":"+RPC_PORT)
	if err != nil {
		return err
	}

	var reply string
	err = client.Call("HyDFSReq.Merge", args, &reply)
	if err != nil {
		return err
	}

	return nil
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
				return
			}
			ts := time.Now()
			for {
				err := sendAppendToQuorum(AppendArgs{hyDFSFilename, fileContent, ts.String(), currentVM}, routingTable[hash(hyDFSFilename)])
				if err == nil {
					break
				}
				removeFileFromCache(hyDFSFilename)
        fmt.Println("evicted from cache")
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
				return
			}
			for {
				err := sendCreateToQuorum(CreateArgs{hyDFSFilename, fileContent}, routingTable[hash(hyDFSFilename)])
				if err == nil {
					break
				}
				removeFileFromCache(hyDFSFilename)
        fmt.Println("evicted from cache")
			}
		case "get":
			if len(args) < 2 {
				fmt.Println("Error: Insufficient arguments. Usage: get HyDFSfilename localfilename")
				continue
			}
			hyDFSFilename := args[0]
			localFilename := args[1]
      if cachedContent, ok := readFileFromCache(hyDFSFilename); ok {
        fmt.Println("Serving from cache")
        err := writeFile(localFilename, cachedContent, "client")
        if err != nil {
          fmt.Printf("Error on file receipt: %v\n", err)
        } else {
          fmt.Printf("File content saved successfully to %s from cache\n", localFilename)
        }
        continue
      }
			var fileContent string

			for {
				fileContent = sendGetToQuorum(GetArgs{hyDFSFilename}, routingTable[hash(hyDFSFilename)])
				if fileContent != "" {
					break
				}
			}
			fmt.Println("I sent a GET message")

			err := writeFile(localFilename, fileContent, "client")
			if err != nil {
				fmt.Printf("Error on file receipt: %v\n", err)
			} else {
				fmt.Printf("File content saved successfully to %s\n", localFilename)
			}
		case "merge":
			if len(args) < 1 {
				fmt.Println("Error: Insufficient arguments. Usage: merge HyDFSfilename")
				continue
			}
			hyDFSFilename := args[0]
			for {
				err := sendMerge(MergeArgs{hyDFSFilename}, hash(hyDFSFilename))
				if err == nil {
					break
				}
			}
		case "list_successors":
			printSuccessors()
		case "routing_table":
			printRoutingTable()
		case "list_local":
			files, err := os.ReadDir("./client")
			if err != nil {
				fmt.Printf("Error reading local directory: %v\n", err)
			} else {
				fmt.Println("Local Files:")
				for _, file := range files {
					fmt.Println(file.Name())
				}
			}
		case "getfromreplica":
			if len(args) < 3 {
				fmt.Println("Error: Insufficient arguments. Usage:  VMAddress HyDFSfilename localfilename")
				continue
			}
			vmAddress := args[0]
			HyDFSfilename := args[1]
			localfilename := args[2]
			fmt.Println("Downloading file from replica...")
			contents, _ := sendGet(GetArgs{HyDFSfilename}, vmAddress)
			err := writeFile(localfilename, contents, "client")
			if err != nil {
				fmt.Printf("Error on file receipt: %v\n", err)
			} else {
				fmt.Printf("File content saved successfully to %s\n", localfilename)
			}
		case "ls":
			if len(args) < 1 {
				fmt.Println("Error: Insufficient arguments. Usage: ls HyDFSfilename")
				continue
			}
			hyDFSFilename := args[0]
			fileHash := hash(hyDFSFilename)
			fmt.Printf(
				"File %s is being stored at - %d address is %s\n",
				hyDFSFilename,
				routingTable[fileHash],
				vmToIP(routingTable[fileHash]),
			)
			//@TODO: this returns only the first owner of the file
		case "store":
			files, err := os.ReadDir("./server")
			if err != nil {
				fmt.Printf("Error reading server directory: %v\n", err)
			} else {
				fmt.Println("Server Files:")
				for _, file := range files {
					fmt.Printf("File name %s has the hash of %d \n", file, hash((file.Name())))
				}
			}
		default:
			fmt.Println("Unknown command")
		}
	}
}
