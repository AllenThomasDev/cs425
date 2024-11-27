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

func sendRemove(args RemoveArgs, ip string) error {
	client, err := rpc.DialHTTP("tcp", ip+":"+RPC_PORT)
	if err != nil {
		return err
	}

	var reply string
	err = client.Call("HyDFSReq.Remove", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

func sendRemoveToQuorum(args RemoveArgs, hash int) error {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()
	if len(successors) <= 3 {
		for i := 0; i < len(successors); i++ {
			err := sendRemove(args, vmToIP(successors[i]))
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
			err := sendRemove(args, vmToIP(successors[i]))
			if err != nil {
				return err
			}
		}
		return nil
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
				err := sendAppendToQuorum(AppendArgs{modifiedFilename, fileContent, ts.String(), currentVM}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
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
				err := sendCreateToQuorum(CreateArgs{modifiedFilename, fileContent}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
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
			startTime := time.Now()
			servedFromCache := false
			hyDFSFilename := args[0]
			localFilename := args[1]
			if cachedContent, ok := readFileFromCache(hyDFSFilename); ok {
				fmt.Println("Serving from cache")
				fmt.Println("GET (from cache) completed")
				duration := time.Since(startTime)
				servedFromCache = true
				logger.Printf("GET took %s, cache status %v \n", duration, servedFromCache)
				err := writeFile(localFilename, cachedContent, "client")
				if err != nil {
					fmt.Printf("Error on file receipt: %v\n", err)
				} else {
					logger.Printf("File content saved successfully to %s from cache\n", localFilename)
				}
				continue
			}
			logger.Println("I sent a GET message")

			modifiedFilename := slashesToBackticks(hyDFSFilename)
			var fileContent string
			var err error
			for {
				fileContent, err = sendGetToQuorum(GetArgs{modifiedFilename}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
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
			duration := time.Since(startTime)
			logger.Printf("GET took %s, cache status %v \n", duration, servedFromCache)
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
				err := sendMerge(MergeArgs{modifiedFilename}, hash(modifiedFilename, MACHINES_IN_NETWORK))
				if err == nil {
					break
				}
			}
		case "cache_size":
			if len(args) < 1 {
				fmt.Println("Error: Insufficient arguments. Usage: cache_size <number_of_entries>")
				continue
			}
			size, err := strconv.Atoi(args[0])
			if err != nil {
				fmt.Printf("Error: Invalid cache size. Please provide a valid number: %v\n", err)
				continue
			}
			if size < 0 {
				fmt.Println("Error: Cache size cannot be negative")
				continue
			}
			// Clear existing cache
			cache.Purge()
			// Create new cache with new size
      cache.Resize(size)
			fmt.Printf("Cache cleared and size updated to %d entries\n", size)
    case "cache_len":
      fmt.Printf("lensize of cache is %d entries\n", cache.Len())
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
			fileHash := hash(modifiedFilename, MACHINES_IN_NETWORK)

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
		case "store":
			fmt.Printf("I have the hash %d\n", ipToVM(selfIP))
			fmt.Println("Server Files:")
			for k := range fileLogs {
				fmt.Printf("File name %s has the hash of %d \n", backticksToSlashes(k), hash(k, MACHINES_IN_NETWORK))
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
		case "RainStorm":
			if len(args) < 7 {
				fmt.Println("Error: Insufficient arguments. Usage: <op1_exe> <op1_type> <op2_exe> <op2_type> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>")
				continue
			}
			op1_exe := args[0]
			op1_type := args[1]
			op2_exe := args[2]
			op2_type := args[3]
			hydfs_src_file := args[4]
			hydfs_dest_file := args[5]
			num_tasks, _ := strconv.Atoi(args[6]) 
			initRainstorm(op1_exe, op1_type, op2_exe, op2_type, hydfs_src_file, hydfs_dest_file, num_tasks)
		default:
			fmt.Println("Unknown command")
		}
	}
}

var backgroundCommand = func(input string) error {
	args := strings.Split(input, " ")
	if len(args) < 1 {
		return fmt.Errorf("Error: need at least one command\n")
	}
	command := args[0]
	args = args[1:] // Extract the arguments after the command
	switch command {
	case "append":
		if len(args) < 2 {
			return fmt.Errorf("Error: Insufficient arguments. Usage: append localfilename HyDFSfilename")
		}
		localFilename := args[0]
		hyDFSFilename := args[1]
		modifiedFilename := slashesToBackticks(hyDFSFilename)
		fileContent, err := readFileToMessageBuffer(localFilename, "client")
		if err != nil {
			return fmt.Errorf("Error reading file: %v\n", err)
		}
		ts := time.Now()
		for {
			err := sendAppendToQuorum(AppendArgs{modifiedFilename, fileContent, ts.String(), currentVM}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
			if err == nil {
				break
			}
			removeFileFromCache(hyDFSFilename)
		}
		removeFileFromCache(hyDFSFilename)
	// swap content of file we want to append with string
	case "appendstring" :
		if len(args) < 2 {
			return fmt.Errorf("Error: Insufficient arguments. Usage: appendstring writeString HyDFSfilename")
		}
		fileContent := args[0]

		hyDFSFilename := args[1]
		modifiedFilename := slashesToBackticks(hyDFSFilename)
		ts := time.Now()
		for {
			err := sendAppendToQuorum(AppendArgs{modifiedFilename, fileContent, ts.String(), currentVM}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
			if err == nil {
				break
			}
			removeFileFromCache(hyDFSFilename)
		}
		removeFileFromCache(hyDFSFilename)
	case "create":
		if len(args) != 2 {
			return fmt.Errorf("Error: Incorrect arguments. Usage: create localfilename HyDFSfilename")
		}
		localFilename := args[0]
		hyDFSFilename := args[1]
		modifiedFilename := slashesToBackticks(hyDFSFilename)
		fileContent, err := readFileToMessageBuffer(localFilename, "client")
		if err != nil {
			return fmt.Errorf("Error reading file: %v\n", err)
		}
		for {
			err := sendCreateToQuorum(CreateArgs{modifiedFilename, fileContent}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
			if err == nil {
				break
			}
			removeFileFromCache(hyDFSFilename)
		}
		removeFileFromCache(hyDFSFilename)
	// swap content of file we want to create with string 
	case "createstring":
		if len(args) != 2 {
			return fmt.Errorf("Error: Incorrect arguments. Usage: create localfilename HyDFSfilename")
		}
		fileContent := args[0]
		hyDFSFilename := args[1]
		modifiedFilename := slashesToBackticks(hyDFSFilename)
		for {
			err := sendCreateToQuorum(CreateArgs{modifiedFilename, fileContent}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
			if err == nil {
				break
			}
			removeFileFromCache(hyDFSFilename)
		}
		removeFileFromCache(hyDFSFilename)
	case "createemptyfile":
		if len(args) != 1 {
			return fmt.Errorf("Error: Incorrect arguments. Usage: create HyDFSfilename")
		}
		hyDFSFilename := args[0]
		modifiedFilename := slashesToBackticks(hyDFSFilename)
		for {
			err := sendCreateToQuorum(CreateArgs{modifiedFilename, ""}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
			if err == nil {
				break
			}
			removeFileFromCache(hyDFSFilename)
		}
		removeFileFromCache(hyDFSFilename)
	case "get":
		if len(args) < 2 {
			return fmt.Errorf("Error: Insufficient arguments. Usage: get HyDFSfilename localfilename")
		}
		hyDFSFilename := args[0]
		localFilename := args[1]
		if cachedContent, ok := readFileFromCache(hyDFSFilename); ok {
			err := writeFile(localFilename, cachedContent, "client")
			if err != nil {
				return fmt.Errorf("Error on file receipt: %v\n", err)
			}
			return nil
		}

		modifiedFilename := slashesToBackticks(hyDFSFilename)
		var fileContent string
		var err error
		for {
			fileContent, err = sendGetToQuorum(GetArgs{modifiedFilename}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
			// if we get an error that isn't the file not existing, keep trying, otherwise give up
			if err != nil {
				return err
			} else {
				break
			}
		}

		err = writeFile(localFilename, fileContent, "client")
		if err != nil {
			return fmt.Errorf("Error on file write: %v\n", err)
		} else {
			addFileToCache(hyDFSFilename, fileContent)
		}
	case "merge":
		if len(args) < 1 {
			return fmt.Errorf("Error: Insufficient arguments. Usage: merge HyDFSfilename")
		}
		hyDFSFilename := args[0]
		modifiedFilename := slashesToBackticks(hyDFSFilename)
		for {
			err := sendMerge(MergeArgs{modifiedFilename}, hash(modifiedFilename, MACHINES_IN_NETWORK))
			if err == nil {
				break
			}
		}
	case "remove":
		if len(args) != 1 {
			return fmt.Errorf("Error: Incorrect arguments. Usage: remove HyDFSfilename")
		}
		hyDFSFilename := args[0]
		modifiedFilename := slashesToBackticks(hyDFSFilename)
		for {
			err := sendRemoveToQuorum(RemoveArgs{[]string{modifiedFilename}}, routingTable[hash(modifiedFilename, MACHINES_IN_NETWORK)])
			if err == nil {
				break
			}
		}
	default:
		return fmt.Errorf("Unknown command")
	}

	return nil
}
