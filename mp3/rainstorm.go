package main

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
)

// can use line number + operation within line (i.e. first operation, second operation, etc.) as unique identifier for subsequent stages beyond source

// TODO: add common wrapper for operations (can handle Transform, FilteredTransform, and AggregateByKey), handle failures!

var (
	topologyArray [][]int
)

func rainstormMain (op1_exe string, op2_exe string, hydfs_src_file string, hydfs_dest_file string, num_tasks int) {
	fmt.Println("Starting Rainstorm")
	genTopology(num_tasks)
	go startRPCListenerScheduler()
	
	sourceArgs, err := createFileChunks(len(topologyArray[0]), hydfs_src_file)
	if err != nil {
		fmt.Printf("Error breaking file into chunks: %v\n", err)
		return
	}

	backgroundCommand("createemptyfile source.log")
	for i := 0; i < len(sourceArgs.StartLines); i++ {
		client, err := rpc.DialHTTP("tcp", vmToIP(topologyArray[0][i])+":"+RPC_PORT)
		if err != nil {
			fmt.Printf("Failed to dial source: %v\n", err)
			return
		}

		var reply string
		err = client.Call("HyDFSReq.Source", SourceArgs{
			hydfs_src_file,
			"source.log",
			sourceArgs.StartLines[i],
			sourceArgs.StartChars[i],
			sourceArgs.LinesPerSource[i],
		}, &reply)
		if err != nil {
			fmt.Printf("Failed to initiate Rainstorm: %v\n", err)
		}
	}

	// remove log files after operation completes (not doing now for debugging purposes)
} 

func genTopology(num_tasks int) {
	// clear topologyArray
	topologyArray = make([][]int, 3, 3)
	
	// use successors (not member list) to determine number of members since rainstorm is just another hydfs command
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()

	// first, determine if we have enough nodes in network to start rainstorm properly
	if len(successors) - 1 < RAINSTORM_LAYERS {
		fmt.Printf("Cannot start Rainstorm: need at least %d worker nodes, have %d in network\n", RAINSTORM_LAYERS, len(successors) - 1)
		return
	}

	nodes_per_layer := (len(successors) - 1)/RAINSTORM_LAYERS
	if nodes_per_layer >= num_tasks {
		nodes_per_layer = num_tasks
		for i := 0; i < RAINSTORM_LAYERS; i++ {
			for j := 0; j < nodes_per_layer; j++ {
				topologyArray[i] = append(topologyArray[i], successors[i * nodes_per_layer + j])	
			}
		}
	} else {
		fmt.Printf("Not enough nodes in network for %d tasks per stage, adjusting topology\n", num_tasks)
		// populate layers of our topology evenly
		for i := 0; i < RAINSTORM_LAYERS; i++ {
			for j := 0; j < nodes_per_layer; j++ {
				topologyArray[i] = append(topologyArray[i], successors[i * nodes_per_layer + j])	
			}
		}
		// distribute remaining nodes (if we have any)
		remaining_nodes := (len(successors) - 1) - (nodes_per_layer * RAINSTORM_LAYERS)
		for i := 0; i < remaining_nodes; i++ {
			topologyArray[i] = append(topologyArray[i], successors[nodes_per_layer * RAINSTORM_LAYERS + i])
		}
	}
	
	for i := 0; i < RAINSTORM_LAYERS; i++ {
		fmt.Printf("LAYER %d: ", i)
		for j := 0; j < len(topologyArray[i]); j++ {
			fmt.Printf("%d ", topologyArray[i][j])
		}
		fmt.Printf("\n")
	}
}

func searchTopology(node int) topology_entry_t {
	for i := 0; i < len(topologyArray); i++ {
		for j := 0; j < len(topologyArray[i]); j++ {
			if topologyArray[i][j] == node {
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

func initRainstorm(op1_exe string, op2_exe string, hydfs_src_file string, hydfs_dest_file string, num_tasks int) {
	if selfIP == introducerIP {
		rainstormMain(op1_exe, op2_exe, hydfs_src_file, hydfs_dest_file, num_tasks)
	} else {
		client, err := rpc.DialHTTP("tcp", introducerIP+":"+RPC_PORT)
		if err != nil {
			fmt.Printf("Failed to dial introducer: %v\n", err)
			return
		}

		var reply string
		err = client.Call("HyDFSReq.StartRainstormRemote", StartRainstormRemoteArgs{op1_exe, op2_exe, hydfs_src_file, hydfs_dest_file, num_tasks}, &reply)
		if err != nil {
			fmt.Printf("Failed to initiate Rainstorm: %v\n", err)
		}
	}
}
