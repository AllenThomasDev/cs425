package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type HyDFSReq string

type CreateArgs struct {
	HyDFSFilename string
	FileContent string
}

type AppendArgs struct {
	HyDFSFilename string
	FileContent string
	Timestamp string
	CallerVM int
}

type GetArgs struct {
	HyDFSFilename string
}

type MergeArgs struct {
	HyDFSFilename string
}

type ForwardedMergeArgs struct {
	HyDFSFilename string
	FileLog []Append_id_t
}

type RemoveArgs struct {
	RemFiles []string
}

type StartRainstormRemoteArgs struct {
	Op1_exe string
	Op1_type Task_type_t
	Op1_args string
	Op2_exe string
	Op2_type Task_type_t
	Op2_args string
	Hydfs_src_file string
	Hydfs_dest_file string
	Num_tasks int
}

type SourceArgs struct {
	SrcFilename string
	LogFilename string
	StartLine int
	StartCharacter int
	LinesToRead int
}

type OpArgs struct {
	ExecFilename string
	IsStateful bool
	StateFilename string
	IsOutput bool
	OutputFilename string
	LogFilename string
}

type InitializeOperatorArgs struct {
	OperatorName	string
	OpType			Task_type_t
	ExecName		string
	Port			string
	LogFile			string
	StateFile		string
	Hash			int
	Numtasks		int
	Args			string
}

func startRPCListenerHyDFS() {
	hydfsreq := new(HyDFSReq)
	rpc.Register(hydfsreq)
	rpc.HandleHTTP()
	servePort, err := net.Listen("tcp", ":" + RPC_PORT)
	if err != nil {
		panic(err)
	}
	go http.Serve(servePort, nil)
}

func (h *HyDFSReq) Get(args *GetArgs, reply *string) error {
	// fmt.Printf("Received GET message to fetch %s\n", args.HyDFSFilename)
	fileContent, err := readFileToMessageBuffer(args.HyDFSFilename, "server")
	if err != nil {
		fmt.Printf("Error writing file content: %v\n", err)
		*reply = ""
		return err
	}

	// get shards
	for i := 0; i < len(fileLogs[args.HyDFSFilename]); i++ {
		// REMINDER: aIDtoFile uses filename, append id to give us randomized filename
		shardContent, err := readFileToMessageBuffer(aIDtoFile[args.HyDFSFilename][fileLogs[args.HyDFSFilename][i]], "server")
		if err != nil {
			fmt.Printf("Error writing file content: %v\n", err)
			*reply = ""
			return err
		}
		fileContent = fileContent + shardContent
	}

	*reply = fileContent
	// fmt.Printf("Sent file content\n")
	return nil
}

func (h *HyDFSReq) Create(args *CreateArgs, reply *string) error {
	// fmt.Printf("Received CREATE message for %s\n", args.HyDFSFilename)

	err := writeFile(args.HyDFSFilename, args.FileContent, "server")
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		} else {
			fmt.Printf("Error on file creation: %v\n", err);
			return err
		}
	}

	fileChannels[args.HyDFSFilename] = make(chan Append_id_t, 100)
	fileLogs[args.HyDFSFilename] = make([]Append_id_t, 0)
	fileLogMutexes[args.HyDFSFilename] = &sync.Mutex{}
	aIDtoFile[args.HyDFSFilename] = make(map[Append_id_t]string, 0)
	// launch thread to manage appends
	go writeToLog(args.HyDFSFilename)

	// fmt.Println("CREATE completed")
	return nil
}

func (h *HyDFSReq) Append(args *AppendArgs, reply *string) error {
	rainstormLog.Printf("Received APPEND message from VM %d\n", args.CallerVM)
	
	aID := Append_id_t{args.CallerVM, args.Timestamp}
	_, exists := aIDtoFile[args.HyDFSFilename][aID]
	if !exists {
		randFilename, err := appendFile(args.HyDFSFilename, args.FileContent)
		if err != nil {
			fmt.Printf("Error appending file\n")
			return err
		}

		logger.Printf("Append to %s at file %s from vm %d\n", args.HyDFSFilename, randFilename, args.CallerVM)
		fileChannels[args.HyDFSFilename] <- aID
		aIDtoFile[args.HyDFSFilename][aID] = randFilename
	}

	// fmt.Println("APPEND completed")
	return nil
}

func (h *HyDFSReq) Merge(args *MergeArgs, reply *string) error {
	fmt.Printf("Merging file %s\n", args.HyDFSFilename)
	fileLogMutexes[args.HyDFSFilename].Lock()
	forwardMerge(args.HyDFSFilename)
	err := mergeFile(args.HyDFSFilename, fileLogs[args.HyDFSFilename])
	fileLogMutexes[args.HyDFSFilename].Unlock()
	if err != nil {
		fmt.Printf("Error merging file: %v\n", err)
		return err
	}
	return nil
}

func (h *HyDFSReq) ForwardedMerge(args *ForwardedMergeArgs, reply *string) error {
	logger.Printf("Merging file %s\n", args.HyDFSFilename)
	fileLogMutexes[args.HyDFSFilename].Lock()
	err := mergeFile(args.HyDFSFilename, args.FileLog)
	fileLogMutexes[args.HyDFSFilename].Unlock()
	if err != nil {
		fmt.Printf("Error merging file: %v\n", err)
		return err
	}
	return nil
}

func (h *HyDFSReq) Remove(args *RemoveArgs, reply *string) error {
	return removeFiles(args.RemFiles)
}

func (h *HyDFSReq) StartRainstormRemote(args *StartRainstormRemoteArgs, reply *string) error {
	go rainstormMain(args.Op1_exe, args.Op1_type, args.Op1_args, args.Op2_exe, args.Op2_type, args.Op2_args, args.Hydfs_src_file, args.Hydfs_dest_file, args.Num_tasks)
	return nil
}

func (h *HyDFSReq) FindFreePort(args struct{}, reply *string) error {
    listener, err := net.Listen("tcp", ":0")
    if err != nil {
        return err
    }
    defer listener.Close()
    port := listener.Addr().(*net.TCPAddr).Port
    *reply = strconv.Itoa(port)
    return nil
}

func (h *HyDFSReq) InitializeOperatorOnPort(args *InitializeOperatorArgs, reply *string) error {
  portString := args.Port
  rainstormLog.Printf("Operator name: %s\n", args.OperatorName)
  rainstormLog.Printf("Operator hash: %d\n", args.Hash)

  // initialize program infrastructure
	var uBuf = make([]string, 0)
  for i := 0; i < args.Numtasks; i++ {
	uBuf = append(uBuf, EMPTY)
  }
  opData := OperatorData{
    Input:  make(chan InputInfo, args.Numtasks), // we can have at most numTasks senders at a time trying to send us data
    Output: make(chan OutputInfo),
	Death: make(chan bool),
	RecvdAck: make(chan string),
	SendAck: make(chan bool),
	StateMap: make(map[string]string),
	LogFile: args.LogFile, // use temp file name so if we already have a local copy of a certain file, get still works
	StateFile: args.StateFile,
	Hash: args.Hash,
	Op: args.OperatorName,
	OpType: args.OpType,
	Exec: args.ExecName,
	UIDBuf: &uBuf,
	UIDBufLock: &sync.Mutex{},
  }
  portToOpData[portString] = opData	

  // this listener will send the tuples to the input channel for this port
  go startRPCListenerWorker(portString, opData.Death)
		
  // restore state from state file if operator is stateful
	if args.OpType == STATEFUL {
		restoreState(args.StateFile, args.Port)
	}

	// get local copy of logfile so we can avoid refetching the log every operation
	err := removeOldLog(args.LogFile)
	if err != nil {
		return err
	}

	err = backgroundCommand(fmt.Sprintf("get %s %s", args.LogFile, "local_logs/" + args.LogFile))
	if err != nil {
		return err
	}

  // create I/O channels
  rainstormLog.Printf("created a channel to listen to inputs, \nthe port here is %s\n", args.Port)
  go processInputChannel(opData, args.Port, args.Args)
  go processOutputChannel(opData, args.Port)
  return nil
}
