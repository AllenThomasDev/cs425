package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
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
				if err.Error() == "NO EXIST\n" {
					fmt.Printf("File %s does not exist\n", backticksToSlashes(args.HyDFSFilename))
					return nil
				}
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
				if err.Error() == "NO EXIST\n" {
					fmt.Printf("File %s does not exist\n", backticksToSlashes(args.HyDFSFilename))
					return nil
				}
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
func sendGetToQuorum(args GetArgs, hash int) (string, error) {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()

	var reply string
	var err error
	
	if len(successors) <= 3 {
		
		for i := 0; i < len(successors); i++ {
			reply, err = sendGet(args, vmToIP(successors[i]))
			if err != nil {
				continue
			}

			return reply, nil
		}
		return "", err
	} else {
		routingTableMutex.RLock()
		defer routingTableMutex.RUnlock()

		_, baseIndex := searchSuccessors(hash)
		for i := baseIndex; i != (baseIndex+3)%len(successors); i = (i + 1) % len(successors) {
			reply, err = sendGet(args, vmToIP(successors[i]))
			if err != nil {
				continue
			}

			return reply, nil
		}
		return "", err
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
			deleteFilesOnServer()
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
			modifiedFilename := slashesToBackticks(hyDFSFilename)
			fileContent, err := readFileToMessageBuffer(localFilename, "client")
			if err != nil {
				fmt.Printf("Error reading file: %v\n", err)
				return
			}
			ts := time.Now()
			for {
				err := sendAppendToQuorum(AppendArgs{modifiedFilename, fileContent, ts.String(), currentVM}, routingTable[hash(modifiedFilename)])
				if err == nil {
					break
				}
				removeFileFromCache(hyDFSFilename)
				logger.Println("evicted from cache")
			}
			fmt.Println("APPEND completed")
			removeFileFromCache(hyDFSFilename)
			logger.Println("evicted from cache")
		case "create":
			if len(args) != 2 {
				fmt.Println("Error: Incorrect arguments. Usage: create localfilename HyDFSfilename")
				continue
			}
			localFilename := args[0]
			hyDFSFilename := args[1]
			modifiedFilename := slashesToBackticks(hyDFSFilename)
			fileContent, err := readFileToMessageBuffer(localFilename, "client")
			if err != nil {
				fmt.Printf("Error reading file: %v\n", err)
				return
			}
			for {
				err := sendCreateToQuorum(CreateArgs{modifiedFilename, fileContent}, routingTable[hash(modifiedFilename)])
				if err == nil {
					break
				}
				removeFileFromCache(hyDFSFilename)
				logger.Println("evicted from cache")
			}
			fmt.Println("CREATE completed")
			removeFileFromCache(hyDFSFilename)
			logger.Println("evicted from cache")
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
					logger.Printf("File content saved successfully to %s from cache\n", localFilename)
					fmt.Println("GET (from cache) completed")
				}
				continue
			}
			logger.Println("I sent a GET message")

			modifiedFilename := slashesToBackticks(hyDFSFilename)
			var fileContent string
			var err error
			for {
				fileContent, err = sendGetToQuorum(GetArgs{modifiedFilename}, routingTable[hash(modifiedFilename)])
				// if we get an error that isn't the file not existing, keep trying, otherwise give up
				if err != nil {
					if err.Error() == "NO EXIST\n" {
						fmt.Printf("File %s does not exist\n", hyDFSFilename)
						break
					}
				} else {
					break
				}
			}

			if err != nil && err.Error() == "NO EXIST\n" {
				continue
			}

			fmt.Println("GET completed")

			err = writeFile(localFilename, fileContent, "client")
			if err != nil {
				fmt.Printf("Error on file write: %v\n", err)
			} else {
				logger.Printf("File content saved successfully to %s\n", localFilename)
				addFileToCache(hyDFSFilename, fileContent)
			}
		case "merge":
			if len(args) < 1 {
				fmt.Println("Error: Insufficient arguments. Usage: merge HyDFSfilename")
				continue
			}
			hyDFSFilename := args[0]
			modifiedFilename := slashesToBackticks(hyDFSFilename)
			for {
				err := sendMerge(MergeArgs{modifiedFilename}, hash(modifiedFilename))
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
				fmt.Println("Error: Insufficient arguments. Usage: VMAddress HyDFSfilename localfilename")
				continue
			}

			vmAddress, _ := strconv.Atoi(args[0])
			HyDFSfilename := args[1]
			modifiedFilename := slashesToBackticks(HyDFSfilename)
			localfilename := args[2]

			logger.Println("Downloading file from replica...")

			contents, err := sendGet(GetArgs{modifiedFilename}, vmToIP(vmAddress))
			if err != nil {
				fmt.Printf("Error: Failed to retrieve file from %d. Reason: %v\n", vmAddress, err)
				continue
			}

			if len(contents) == 0 {
				fmt.Println("Warning: Received empty file content. File may not exist on the replica.")
				continue
			}

			// Attempt to write the file to the local system
			err = writeFile(localfilename, contents, "client")
			if err != nil {
				fmt.Printf("Error: Failed to save file content to %s. Reason: %v\n", localfilename, err)
			} else {
				logger.Printf("File content saved successfully to %s\n %s", localfilename, contents)
			}

			fmt.Println("GET from replica completed")
		case "ls":
			if len(args) < 1 {
				fmt.Println("Error: Insufficient arguments. Usage: ls HyDFSfilename")
				continue
			}
			hyDFSFilename := args[0]
			modifiedFilename := slashesToBackticks(hyDFSFilename)
			fileHash := hash(modifiedFilename)

			fmt.Printf("File %s is stored on the following machines:\n", hyDFSFilename)

			// Lock for safe access to successors and routing table
			routingTableMutex.RLock()
			successorsMutex.RLock()

			_, baseIndex := searchSuccessors(fileHash)

			n := 3 // Assuming 3 replicas; adjust based on your replication factor
			for i := 0; i < n; i++ {
				successorIndex := (baseIndex + i) % len(successors)
				vmID := successors[successorIndex]
				vmIP := vmToIP(vmID)
				fmt.Printf("VM Address: %s, VM ID on ring: %d\n", vmIP, vmID)
			}

			routingTableMutex.RUnlock()
			successorsMutex.RUnlock()
			//@TODO: this returns only the first owner of the file
		case "store":
			fmt.Printf("I have the hash %d\n", ipToVM(selfIP))
			fmt.Println("Server Files:")
			for k := range(fileLogs) {
				fmt.Printf("File name %s has the hash of %d \n", backticksToSlashes(k), hash(k))
			}
		case "merge_perf":
			if len(args) < 3 {
				fmt.Println("Error: Insufficient arguments. Usage: HyDFSFilename append_size conc_appends")
				continue
			}
			filename := args[0]
			append_size, _ := strconv.Atoi(args[1])
			conc_appends, _ := strconv.Atoi(args[2])
			gen_merge_files(filename, append_size, conc_appends)
		default:
			fmt.Println("Unknown command")
		}
	}
}
