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
	Op2_exe string
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

type TaskArgs struct {
	TaskType Task_type_t
	SA SourceArgs
	OA OpArgs
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
	fmt.Printf("Received GET message to fetch %s\n", args.HyDFSFilename)
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
	fmt.Printf("Sent file content\n")
	return nil
}

func (h *HyDFSReq) Create(args *CreateArgs, reply *string) error {
	fmt.Printf("Received CREATE message for %s\n", args.HyDFSFilename)

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

	fmt.Println("CREATE completed")
	return nil
}

func (h *HyDFSReq) Append(args *AppendArgs, reply *string) error {
	fmt.Println("Received APPEND message")
	
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

	fmt.Println("APPEND completed")
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
	go rainstormMain(args.Op1_exe,  args.Op2_exe, args.Hydfs_src_file, args.Hydfs_dest_file, args.Num_tasks)
	return nil
}

func (h *HyDFSReq) StartTask(args *TaskArgs, reply *string) error {
	freePort, err := getFreePort()
	if err != nil {
		return err
	}

	portString := strconv.Itoa(freePort)
	*reply = portString

	// Start serving worker functions from passed port
	go startRPCListenerWorker(portString)
	// initialize channel for stopping task due to rescheduling
	stopChannels[portString] = make(chan string)
	
	if args.TaskType == OP {
		fmt.Printf("Executing op %s, stateful = %t, output = %t\n", args.OA.ExecFilename, args.OA.IsStateful, args.OA.IsOutput)
		go opWrapper(args.OA, portString)
		return nil
	} else if args.TaskType == SOURCE {
		fmt.Printf("Processing %d lines of %s starting at line %d\n", args.SA.LinesToRead, args.SA.SrcFilename, args.SA.StartLine)
    tuples  := generateSourceTuples(args.SA.SrcFilename, args.SA.LogFilename, args.SA.StartLine, args.SA.StartCharacter, args.SA.LinesToRead, portString)
    for _, tuple := range(tuples){
      fmt.Println(tuple)
      sendToNextStage(ArgsWithSender{tuple, currentVM, portString})
    }
		return nil
	} else {
		return fmt.Errorf("Unknown task type")
	}
}
