package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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

func startRPCListener() {
	logFile, err := os.OpenFile("server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Failed to open log file:", err)
		os.Exit(1)
	}
	defer logFile.Close()
	logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)

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
	forwardMerge(args.HyDFSFilename)
	err := mergeFile(args.HyDFSFilename, fileLogs[args.HyDFSFilename])
	if err != nil {
		fmt.Printf("Error merging file: %v\n", err)
		return err
	}
	return nil
}

func (h *HyDFSReq) ForwardedMerge(args *ForwardedMergeArgs, reply *string) error {
	fmt.Printf("Merging file %s\n", args.HyDFSFilename)
	err := mergeFile(args.HyDFSFilename, args.FileLog)
	if err != nil {
		fmt.Printf("Error merging file: %v\n", err)
		return err
	}
	return nil
}

func (h *HyDFSReq) Remove(args *RemoveArgs, reply *string) error {
	return removeFiles(args.RemFiles)
}