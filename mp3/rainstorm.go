package main

import (
	"fmt"
	"net/rpc"
)

var (
	topologyArray [][]int
)

func rainstormMain (op1_exe string, op2_exe string, hydfs_src_file string, hydfs_dest_file string, num_tasks int) {
	fmt.Println("Starting Rainstorm")
	genTopology(num_tasks)
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

func initRainstorm(op1_exe string, op2_exe string, hydfs_src_file string, hydfs_dest_file string, num_tasks int) {
	if selfIP == introducerIP {
		rainstormMain(op1_exe, op2_exe, hydfs_src_file, hydfs_dest_file, num_tasks)
	} else {
		client, err := rpc.DialHTTP("tcp", introducerIP+":"+RPC_PORT)
		if err != nil {
			fmt.Printf("Failed to dial introducer: %v\n", err)
		}

		var reply string
		err = client.Call("HyDFSReq.StartRainstormRemote", StartRainstormRemoteArgs{op1_exe, op2_exe, hydfs_src_file, hydfs_dest_file, num_tasks}, &reply)
		if err != nil {
			fmt.Printf("Failed to initiate Rainstorm: %v\n", err)
		}
	}
}