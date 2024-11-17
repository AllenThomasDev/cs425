package main

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
)

var (
	topologyArray [][]int
)

func rainstormMain (op1_exe string, op2_exe string, hydfs_src_file string, hydfs_dest_file string, num_tasks int) {
	fmt.Println("Starting Rainstorm")
	genTopology(num_tasks)
	sourceArgs, err := createFileChunks(len(topologyArray[0]), hydfs_src_file)
	if err != nil {
		fmt.Printf("Error breaking file into chunks: %v\n", err)
		return
	}

	// backgroundCommand("createemptyfile source.log")
	for i := 0; i < len(sourceArgs[0]); i++ {
		client, err := rpc.DialHTTP("tcp", vmToIP(topologyArray[0][i])+":"+RPC_PORT)
		if err != nil {
			fmt.Printf("Failed to dial source: %v\n", err)
			return
		}

		var reply string
		err = client.Call("HyDFSReq.Source", SourceArgs{hydfs_src_file, "source.log", sourceArgs[0][i], sourceArgs[1][i], sourceArgs[2][i]}, &reply)
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

func createFileChunks(num_sources int, hydfs_src_file string) ([][]int, error) {
	if num_sources == 0 {
		return nil, fmt.Errorf("No sources to pass chunks to\n")
	} else {
		err := backgroundCommand(fmt.Sprintf("merge %s", hydfs_src_file))
		if err != nil {
			return nil, err
		}

		randomFileName := genRandomFileName()
		err = backgroundCommand(fmt.Sprintf("get %s %s", hydfs_src_file, randomFileName))
		if err != nil {
			return nil, err
		}

		src_file, err := os.OpenFile("client/" + randomFileName, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}

		// total number of lines in file
		lineCount := 0
		// cumulative sum of characters in file at start of each line
		totalChars := 0
		charsAtLine := []int {0}
		b := make([]byte, 1)
		for {
			n1, err := src_file.Read(b)
			if n1 == 0 {
				if err != nil && err != io.EOF {
					return nil, err
				}
				break
			}

			totalChars++

			if string(b) == "\n" {
				charsAtLine = append(charsAtLine, totalChars)
				lineCount++
			}
		}
		
		sourceTotalLines := make([]int, num_sources, num_sources)
		linesPerSource := lineCount/num_sources
		for i := 0; i < num_sources; i++ {
			sourceTotalLines[i] = linesPerSource
		}
		remaining_lines := lineCount - (linesPerSource * num_sources)
		for i := 0; i < remaining_lines; i++ {
			sourceTotalLines[i]++
		}

		sourceStartLines := make([]int, num_sources, num_sources)
		sourceStartCharacters := make([]int, num_sources, num_sources)
		sourceStartLines[0] = 0
		sourceStartCharacters[0] = 0
		for i := 1; i < num_sources; i++ {
			sourceStartLines[i] = sourceStartLines[i - 1] + sourceTotalLines[i - 1]
			sourceStartCharacters[i] = charsAtLine[sourceStartLines[i]];
		}

		os.Remove("client/" + randomFileName)
		return [][]int{sourceStartLines, sourceStartCharacters, sourceTotalLines}, nil
	}
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