package main

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
)

// can use line number + operation within line (i.e. first operation, second operation, etc.) as unique identifier for subsequent stages beyond source

// TODO: add common wrapper for operations (can handle Transform, FilteredTransform, and AggregateByKey), add start channel to all workers
// handle failures!

var (
	topologyArray [][]task_addr_t
	tasksPerWorker = make(map[int]int)
)

func rainstormMain (op1_exe string, op1_type string, op2_exe string, op2_type string, hydfs_src_file string, hydfs_dest_file string, num_tasks int) {
	fmt.Println("Starting Rainstorm")
	go startRPCListenerScheduler()
	
	// determine op args
	var op1Args OpArgs
	var op2Args OpArgs
	if (op1_type != "Transform" && op1_type != "FilteredTransform" && op1_type != "AggregateByKey") ||
		(op2_type != "Transform" && op2_type != "FilteredTransform" && op2_type != "AggregateByKey") {
		fmt.Println("Error: op type must be one of Transform, FilteredTransform, AggregateByKey")
		return
	} else {
		// create file for op1 nodes to log processed IDs
		backgroundCommand("createemptyfile op1.log")
		
		// determine args for op1 nodes
		op1Args.ExecFilename = op1_exe
		op1Args.LogFilename = "op1.log"
		op1Args.IsOutput = false
		op1Args.OutputFilename = ""
		
		if op1_type == "AggregateByKey" {
			backgroundCommand("createemptyfile op1_state.log")
			op1Args.IsStateful = true
			op1Args.StateFilename = "op1_state.log"
		}
		
		// create file for op2 nodes to log processed IDs
		backgroundCommand("createemptyfile op2.log")
		
		// determine args for op2 nodes
		op2Args.ExecFilename = op2_exe
		op2Args.LogFilename = "op2.log"
		op2Args.IsOutput = true
		op2Args.OutputFilename = hydfs_dest_file
		
		if op2_type == "AggregateByKey" {
			backgroundCommand("createemptyfile op2_state.log")
			op2Args.IsStateful = true
			op2Args.StateFilename = "op2_state.log"
		}
	}
	
	// determine topology so we know which nodes to assign to each task to
	nodeTopology := genTopology(num_tasks)
	topologyArray = make([][]task_addr_t, 3, 3)

	// break file into chunks
	sourceArgs, err := createFileChunks(len(nodeTopology[0]), hydfs_src_file)
	if err != nil {
		fmt.Printf("Error breaking file into chunks: %v\n", err)
		return
	}

	// create log for sources
	backgroundCommand("createemptyfile source.log")

	// start sources
	var ta TaskArgs
	for i := 0; i < len(nodeTopology[0]); i++ {
		client, err := rpc.DialHTTP("tcp", vmToIP(nodeTopology[0][i])+":"+RPC_PORT)
		if err != nil {
			fmt.Printf("Failed to dial worker: %v\n", err)
			return
		}

		ta.TaskType = SOURCE
		ta.SA.SrcFilename = hydfs_src_file
		ta.SA.LogFilename = "source.log"
		ta.SA.StartLine = sourceArgs.StartLines[i]
		ta.SA.StartCharacter = sourceArgs.StartChars[i]
		ta.SA.LinesToRead = sourceArgs.LinesPerSource[i]

		var reply string
		err = client.Call("HyDFSReq.StartTask", ta, &reply)
		if err != nil {
			fmt.Printf("Failed to initiate Rainstorm: %v\n", err)
		}

		topologyArray[0] = append(topologyArray[0], task_addr_t{nodeTopology[0][i], reply})
	}

	// start op1
	for i := 0; i < len(nodeTopology[1]); i++ {
		client, err := rpc.DialHTTP("tcp", vmToIP(nodeTopology[1][i])+":"+RPC_PORT)
		if err != nil {
			fmt.Printf("Failed to dial worker: %v\n", err)
			return
		}

		ta.TaskType = OP
		ta.OA = op1Args

		var reply string
		err = client.Call("HyDFSReq.StartTask", ta, &reply)
		if err != nil {
			fmt.Printf("Failed to initiate Rainstorm: %v\n", err)
		}

		topologyArray[1] = append(topologyArray[1], task_addr_t{nodeTopology[1][i], reply})
	}

	// start op2
	for i := 0; i < len(nodeTopology[2]); i++ {
		client, err := rpc.DialHTTP("tcp", vmToIP(nodeTopology[2][i])+":"+RPC_PORT)
		if err != nil {
			fmt.Printf("Failed to dial worker: %v\n", err)
			return
		}

		ta.TaskType = OP
		ta.OA = op2Args

		var reply string
		err = client.Call("HyDFSReq.StartTask", ta, &reply)
		if err != nil {
			fmt.Printf("Failed to initiate Rainstorm: %v\n", err)
		}

		topologyArray[2] = append(topologyArray[2], task_addr_t{nodeTopology[2][i], reply})
	}

	for i := 0; i < RAINSTORM_LAYERS; i++ {
		fmt.Printf("LAYER %d: ", i)
		for j := 0; j < len(topologyArray[i]); j++ {
			fmt.Printf("%d: %s |", topologyArray[i][j].VM, topologyArray[i][j].port)
		}
		fmt.Printf("\n")
	}

	// remove log files after operation completes (not doing now for debugging purposes)
}

func genTopology(num_tasks int) [][]int {
	nodeTopology := make([][]int, 3, 3)
	
	// use successors (not member list) to determine number of members since rainstorm is just another hydfs command
	successorsMutex.RLock()

	for i := 0; i < len(successors) - 1; i++ {
		tasksPerWorker[successors[i]] = 0
	}

	successorsMutex.RUnlock()

	for i := 0; i < RAINSTORM_LAYERS; i++ {
		for j := 0; j < num_tasks; j++ {
			slackerNode := getSlackerNode()
			nodeTopology[i] = append(nodeTopology[i], slackerNode)
			tasksPerWorker[slackerNode]++
		}
	}
	
	for i := 0; i < RAINSTORM_LAYERS; i++ {
		fmt.Printf("LAYER %d: ", i)
		for j := 0; j < len(nodeTopology[i]); j++ {
			fmt.Printf("%d ", nodeTopology[i][j])
		}
		fmt.Printf("\n")
	}

	return nodeTopology
}

// returns the node that currently has the least tasks assigned to it
func getSlackerNode() int {
	minTasks := 10
	minTasksNode := 10
	
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()
	
	for i := 0; i < len(successors) - 1; i++ {
		if tasksPerWorker[successors[i]] < minTasks {
			minTasks = tasksPerWorker[successors[i]]
			minTasksNode = successors[i]
		} 
	}

	return minTasksNode
}

// returns the node that currently has the most tasks assigned to it
func getGrinderNode() int {
	mostTasks := -1
	mostTasksNode := -1

	successorsMutex.RLock()
	defer successorsMutex.RUnlock()
	
	for i := 0; i < len(successors) - 1; i++ {
		if tasksPerWorker[successors[i]] > mostTasks {
			mostTasks = tasksPerWorker[successors[i]]
			mostTasksNode = successors[i]
		} 
	}

	return mostTasksNode
}

func searchTopology(node int) topology_entry_t {
	for i := 0; i < len(topologyArray); i++ {
		for j := 0; j < len(topologyArray[i]); j++ {
			if topologyArray[i][j].VM == node {
				return topology_entry_t{i, j}
			}
		}
	}
	return topology_entry_t{-1, -1}
}

// createFileChunks splits a HydFS file into chunks for multiple sources
func createFileChunks(numSources int, hydfsSourceFile string) (*FileChunkInfo, error) {
	if numSources == 0 {
		return nil, fmt.Errorf("no sources to pass chunks to")
	}

	tempFileName, err := prepareSourceFile(hydfsSourceFile)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare source file: %w", err)
	}

	defer cleanupTempFile(tempFileName)

	fileInfo, err := analyzeFile(tempFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze file: %w", err)
	}

	return distributeLines(fileInfo.lineCount, fileInfo.charsAtLine, numSources)
}

// prepareSourceFile merges and retrieves the source file from HydFS
func prepareSourceFile(hydfsSourceFile string) (string, error) {
	if err := backgroundCommand(fmt.Sprintf("merge %s", hydfsSourceFile)); err != nil {
		return "", err
	}

	tempFileName := genRandomFileName()
	if err := backgroundCommand(fmt.Sprintf("get %s %s", hydfsSourceFile, tempFileName)); err != nil {
		return "", err
	}

	return tempFileName, nil
}

// analyzeFile counts lines and characters in the file
func analyzeFile(fileName string) (*fileAnalysis, error) {
	src, err := os.OpenFile("client/"+fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	lineCount := 0
	totalChars := 0
	charsAtLine := []int{0}
	
	buf := make([]byte, 1)
	for {
		n, err := src.Read(buf)
		if n == 0 {
			if err != nil && err != io.EOF {
				return nil, err
			}
			break
		}
		
		totalChars++
		if string(buf) == "\n" {
			charsAtLine = append(charsAtLine, totalChars)
			lineCount++
		}
	}

	return &fileAnalysis{
		lineCount:   lineCount,
		charsAtLine: charsAtLine,
	}, nil
}

// distributeLines calculates how to distribute lines across sources
func distributeLines(lineCount int, charsAtLine []int, numSources int) (*FileChunkInfo, error) {
	// Calculate lines per source
	linesPerSource := make([]int, numSources)
	baseLines := lineCount / numSources
	for i := range linesPerSource {
		linesPerSource[i] = baseLines
	}

	// Distribute remaining lines
	remainingLines := lineCount - (baseLines * numSources)
	for i := 0; i < remainingLines; i++ {
		linesPerSource[i]++
	}

	// Calculate starting positions
	startLines := make([]int, numSources)
	startChars := make([]int, numSources)
	
	startLines[0] = 0
	startChars[0] = 0
	for i := 1; i < numSources; i++ {
		startLines[i] = startLines[i-1] + linesPerSource[i-1]
		startChars[i] = charsAtLine[startLines[i]]
	}

	return &FileChunkInfo{
		StartLines:     startLines,
		StartChars:     startChars,
		LinesPerSource: linesPerSource,
	}, nil
}

// cleanupTempFile removes the temporary file
func cleanupTempFile(fileName string) {
	os.Remove("client/" + fileName)
}

func initRainstorm(op1_exe string, op1_type string, op2_exe string, op2_type string, hydfs_src_file string, hydfs_dest_file string, num_tasks int) {
	if selfIP == introducerIP {
		rainstormMain(op1_exe, op1_type, op2_exe, op2_type, hydfs_src_file, hydfs_dest_file, num_tasks)
	} else {
		client, err := rpc.DialHTTP("tcp", introducerIP+":"+RPC_PORT)
		if err != nil {
			fmt.Printf("Failed to dial introducer: %v\n", err)
			return
		}

		var reply string
		err = client.Call("HyDFSReq.StartRainstormRemote", StartRainstormRemoteArgs{op1_exe, op1_type, op2_exe, op2_type, hydfs_src_file, hydfs_dest_file, num_tasks}, &reply)
		if err != nil {
			fmt.Printf("Failed to initiate Rainstorm: %v\n", err)
		}
	}
}
