package main

import (
	"fmt"
	"io"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// TODO: make operators write to proper logfile when outputting, have operators check logfile when processing inputs, adjust/remove cache for UIDs

// this is vm:port:operator, needs to be updated when things fail
var currentActiveOperators = make(map[int]map[string]Operator)
var operatorToVmPorts = make(map[string][]task_addr_t)
var availableOperators = make([]string, 2)
var operatorSequence = make([]string, 3)

var (
	// TODO: does this have to be 3*3
	topologyArray   = make([][]task_addr_t, 3, 3)
	tasksPerWorker  = make(map[int]int)
	rainstormArgs   StartRainstormRemoteArgs // save args for rescheduling
	rainstormActive bool                     // flag to enable rescheduling on joins/leaves
)

func rainstormMain(op1 string, op2 string, hydfs_src_file string, hydfs_dest_file string, numTasks int) {
	fmt.Println("Starting Rainstorm ...")
	if valid := validateOperations([]string{op1, op2}); !valid {
		return
	}
	operatorSequence = []string{"source", op1, op2}
  rainstormActive = true
	rainstormArgs = StartRainstormRemoteArgs{
		op1,
		op2,
		hydfs_src_file,
		hydfs_dest_file,
		numTasks,
	}
	distributeTasks(numTasks)
	createLogFiles(operatorSequence)
	backgroundCommand(fmt.Sprintf("createemptyfile %s", hydfs_dest_file))
  // TODO: every time that membership list is updated, we need to update a lot of globals
	go startRPCListenerScheduler()
	go startRPCListenerWorker(CONSOLE_OUT_PORT)
	// here i am making the assumption that a source does not fail before we send the chunks to it
	sourceArgs, err := createFileChunks(numTasks, hydfs_src_file)
	if err != nil {
		fmt.Printf("Error breaking file into chunks: %v\n", err)
		return
	}
	sourceTriggers := convertFileInfoStructListToTuples(hydfs_src_file, *sourceArgs, numTasks)
	startSources(sourceTriggers)
	fmt.Println(currentActiveOperators, sourceTriggers)
	select {}
}

func createLogFiles(operatorSequence []string) {
	for i := 0; i < len(operatorToVmPorts); i++ {
		for j := 0; j < len(operatorToVmPorts[operatorSequence[i]]); j++ {
			backgroundCommand(fmt.Sprintf("createemptyfile %s_%d.log", operatorSequence[i], j))
			backgroundCommand(fmt.Sprintf("createemptyfile %s_%d_state.log", operatorSequence[i], j))
		}
	}
}

func removeLogFiles(operatorSequence []string) {
	for opStr := range operatorSequence {
		for i := 0; i < len(operatorToVmPorts[operatorSequence[opStr]]); i++ {
			backgroundCommand(fmt.Sprintf("remove %s_%d.log", opStr, i))
			backgroundCommand(fmt.Sprintf("remove %s_%d_state.log", opStr, i))
		}
	}
}

func callStartTask(vm int, ta TaskArgs) (string, error) {
	client, err := rpc.DialHTTP("tcp", vmToIP(vm)+":"+RPC_PORT)
	if err != nil {
		return "", err
	}

	var reply string
	err = client.Call("HyDFSReq.StartTask", ta, &reply)
	if err != nil {
		return "", err
	}

	return reply, nil
}

func callInitializeOperatorOnVM(vm int, op string) (string, error) {
	client, err := rpc.DialHTTP("tcp", vmToIP(vm)+":"+RPC_PORT)
	if err != nil {
		fmt.Printf("breaks here, %s", err.Error())
		return "", err
	}
	port, err := callFindFreePort(vm)
	opPort := OperatorPort{
		OperatorName: op,
		Port:         port,
	}
	var reply string
	err = client.Call("HyDFSReq.InitializeOperatorOnPort", opPort, &reply)
	if err != nil {
		fmt.Printf("breaks here, %s", err.Error())
		return "", err
	}
	if currentActiveOperators[vm] == nil {
		currentActiveOperators[vm] = make(map[string]Operator)
	}
	currentActiveOperators[vm][port] = operators[op]
  operatorToVmPorts[op] = append(operatorToVmPorts[op],task_addr_t{vm, port})
  fmt.Print(operatorToVmPorts[op])
	fmt.Printf("Started %s on VM %d:%s\n", operators[op].Name, vm, port)
	return reply, nil
}

func callFindFreePort(vm int) (string, error) {
	client, err := rpc.DialHTTP("tcp", vmToIP(vm)+":"+RPC_PORT)
	if err != nil {
		return "", fmt.Errorf("failed to dial RPC: %v", err)
	}
	defer client.Close()

	var reply string
	err = client.Call("HyDFSReq.FindFreePort", struct{}{}, &reply)
	if err != nil {
		return "", fmt.Errorf("RPC call failed: %v", err)
	}
	if reply == "" {
		return "", fmt.Errorf("received empty port from FindFreePort")
	}
	if _, err := strconv.Atoi(reply); err != nil {
		return "", fmt.Errorf("invalid port format: %s", reply)
	}

	return reply, nil
}

func constructSourceArgs(hydfs_src_file string, startLine int, startCharacter int, linesToRead int) SourceArgs {
	var sa SourceArgs
	sa.SrcFilename = hydfs_src_file
	sa.LogFilename = "source.log"
	sa.StartLine = startLine
	sa.StartCharacter = startCharacter
	sa.LinesToRead = linesToRead
	return sa
}

func constructOp1Args(op1_exe string) OpArgs {
	var op1Args OpArgs
	op1Args.ExecFilename = op1_exe
	op1Args.LogFilename = "op1.log"
	op1Args.IsOutput = false
	op1Args.OutputFilename = ""
	// TODO: handle IsStateful from operator name?
	// if op1_type == "AggregateByKey" {
	// 	op1Args.IsStateful = true
	// 	op1Args.StateFilename = "op1_state.log"
	// }
	return op1Args
}

func constructOp2Args(op2_exe string, hydfs_dest_file string) OpArgs {
	var op2Args OpArgs
	op2Args.ExecFilename = op2_exe
	op2Args.LogFilename = "op2.log"
	op2Args.IsOutput = true
	op2Args.OutputFilename = hydfs_dest_file
	//
	// if op2_type == "AggregateByKey" {
	// 	op2Args.IsStateful = true
	// 	op2Args.StateFilename = "op2_state.log"
	// }
	return op2Args
}

func constructRescheduleArgs(tent topology_entry_t) (TaskArgs, error) {
	var ta TaskArgs
	if tent.Layer == 0 {
		sourceArgs, err := createFileChunks(len(topologyArray[0]), rainstormArgs.Hydfs_src_file)
		if err != nil {
			return TaskArgs{}, err
		}

		ta.TaskType = SOURCE
		ta.SA = constructSourceArgs(rainstormArgs.Hydfs_src_file, sourceArgs.StartLines[tent.Hash], sourceArgs.StartChars[tent.Hash], sourceArgs.LinesPerSource[tent.Hash])
	} else if tent.Layer == 1 {
		ta.TaskType = OP
		ta.OA = constructOp1Args(rainstormArgs.Op1_exe)
	} else {
		ta.TaskType = OP
		ta.OA = constructOp1Args(rainstormArgs.Op2_exe)
	}

	return ta, nil
}

func rebalanceTasksOnNodeFailure(vm int) {
  tasksOnDeadVM := currentActiveOperators[vm]
  var taskAddrToBeDeleted []task_addr_t
  delete(currentActiveOperators, vm)
  var tasksToBeResurrected []Operator
  for port, _ := range(tasksOnDeadVM) {
    tasksToBeResurrected = append(tasksToBeResurrected, tasksOnDeadVM[port])
    taskAddrToBeDeleted = append(taskAddrToBeDeleted, task_addr_t{vm, port})
  }
  for i := range(tasksToBeResurrected){
    // remove address from operatorToVmPorts[]
    operatorName := tasksToBeResurrected[i].Name
    removeTaskAddrFromOperator(operatorName, taskAddrToBeDeleted[i])
    destination := findNodeWithFewestTasks()
		callInitializeOperatorOnVM(destination, operatorName)
  }
}


func removeTaskAddrFromOperator(operatorName string, Addr task_addr_t) {
  // Retrieve the slice of task addresses for the given operator
  taskAddrsForOperators := operatorToVmPorts[operatorName]
  // Iterate through the slice to find the address and remove it
  for i := 0; i < len(taskAddrsForOperators); i++ {
    if taskAddrsForOperators[i] == Addr {
      // Shift all elements to the left, effectively removing the element at index i
      taskAddrsForOperators = append(taskAddrsForOperators[:i], taskAddrsForOperators[i+1:]...)
      break // Exit after removing the address
    }
  }
  //entries are added in this global when a task is initalized
  operatorToVmPorts[operatorName] = taskAddrsForOperators
  fmt.Print("things were updated\n")
  fmt.Print(operatorToVmPorts[operatorName])
}


func findNodeWithFewestTasks() int {
	var shortestKey int
	minLength := -1
	multipleKeys := false

	for key, operatorMap := range currentActiveOperators {
		length := len(operatorMap)
		if minLength == -1 || length < minLength {
			minLength = length
			shortestKey = key
			multipleKeys = false // Reset multiple keys flag
		} else if length == minLength {
			multipleKeys = true
		}
	}
	// If there are multiple shortest keys, pick any random key from the original map
	if multipleKeys {
		keys := make([]int, 0, len(currentActiveOperators))
		for k := range currentActiveOperators {
			keys = append(keys, k)
		}
		return keys[rand.Intn(len(keys))]
	}

	return shortestKey

}



func rescheduleTask(change string, vm int) {

	if change == "LEAVE" {
		hangingTasks := searchTopology(vm)

		for i := 0; i < len(hangingTasks); i++ {
			ta, err := constructRescheduleArgs(hangingTasks[i])
			if err != nil {
				fmt.Printf("Error constructing args for rescheduling: %v\n", err)
				return
			}

			slackerNode := getSlackerNode()
			port, _ := callFindFreePort(slackerNode)
			_, err = callStartTask(slackerNode, ta)
			if err != nil {
				fmt.Printf("Failed to start task on new node: %v\n", err)
				return
			}

			topologyArray[hangingTasks[i].Layer][hangingTasks[i].Hash] = task_addr_t{slackerNode, port}
			tasksPerWorker[slackerNode]++
		}

		// clear failed node
		delete(tasksPerWorker, vm)
	} else {
		grinderNode := getGrinderNode()
		grinderTasks := searchTopology(grinderNode)

		// stop task we wish to reschedule
		if len(grinderTasks) > 0 {
			client, err := rpc.DialHTTP("tcp", vmToIP(topologyArray[grinderTasks[0].Layer][grinderTasks[0].Hash].VM)+":"+topologyArray[grinderTasks[0].Layer][grinderTasks[0].Hash].port)
			if err != nil {
				fmt.Printf("Error dialing grinder: %v\n", err)
				return
			}

			var reply string
			err = client.Call("WorkerReq.StopTask", StopTaskArgs{topologyArray[grinderTasks[0].Layer][grinderTasks[0].Hash].port}, &reply)
			if err != nil {
				fmt.Printf("Error calling StopTask: %v\n", err)
				return
			}

			// reschedule task on newly joined node
			ta, err := constructRescheduleArgs(grinderTasks[0])
			if err != nil {
				fmt.Printf("Error constructing args for rescheduling: %v\n", err)
				return
			}

			port, _ := callFindFreePort(vm)
			_, err = callStartTask(vm, ta)
			if err != nil {
				fmt.Printf("Failed to start task on new node: %v\n", err)
				return
			}

			topologyArray[grinderTasks[0].Layer][grinderTasks[0].Hash] = task_addr_t{vm, port}
			tasksPerWorker[vm] = 1
		}
	}
	fmt.Printf("Rescheduling finished\n")
}

func distributeTasks(numTasks int) {
	memberKeys := make([]int, 0, len(membershipList))
	for key := range membershipList {
		memberKeys = append(memberKeys, ipToVM(key))
	}
	sort.Ints(memberKeys)
	// we need to do this because membership list is a map, and order is not deterministic
	// we need to sort it so that we get rid of leader, we could also do this, by iterating once, this is just easier
	if len(memberKeys) > 1 {
		fmt.Println("We are excluding the leader from the list of members to assign tasks to")
		memberKeys = memberKeys[1:]
	}
	if len(memberKeys) == 0 {
		fmt.Println("No members available.")
		return
	}
	totalOperators := len(operatorSequence)
	totalTasks := numTasks * totalOperators
	memberIndex := 0
	for i := 0; i < totalTasks; i++ {
		operatorIndex := i / numTasks
		operator := operatorSequence[operatorIndex]
		member := memberKeys[memberIndex]
		callInitializeOperatorOnVM(member, operator)
		memberIndex = (memberIndex + 1) % len(memberKeys)
	}
}


func findOperatorFromTaskAddr(taskAddr task_addr_t) string {
	for operatorName, vmPorts := range operatorToVmPorts {
		for _, addr := range vmPorts {
			if addr == taskAddr {
				return operatorName
			}
		}
	}
	panic("couldn't find operator for a certain address")
	// TODO:
	// we could have problems if something dies, we remove it from the operator
	// to VMs map and then we receive a tuple, we won't know where we got it from
	// ,thus maybe we should have 2 maps, one that has all operators and ports
	// mappings that were ever creaeted, the other has only active ones
	// this can be solved if we along with the tuple, we just send the operator that just operated on the tuple, instead of the VM:Port
	// but i am bit tired to make that refactor right now
}

// func findLayerFromTaskAddr(taskAddr task_addr_t) int {
// 	operatorName := findOperatorFromTaskAddr(taskAddr)
// 	for i := 0; i < len(operatorSequence); i++ {
// 		if operatorName == operatorSequence[i] {
// 			return i
// 		}
// 	}
// 	fmt.Printf("Error: couldn't match operator with layer\n")
// 	return -1
// }

func matchTaskWithHash(taskAddr task_addr_t, opString string) int {
	for i := 0; i < len(operatorToVmPorts[opString]); i++ {
		if operatorToVmPorts[opString][i] == taskAddr {
			return i
		}
	}
	return -1
}

func getNextOperator(operatorName string) string {
	for i, name := range operatorSequence {
		if name == operatorName {
			if i == len(operatorSequence)-1 {
				return "completed"
			}
			return operatorSequence[i+1]
		}
	}
	return "not found"
}

func startSources(sourceTriggers []Rainstorm_tuple_t) {
	fmt.Println(operatorToVmPorts)
	sourceAddrs := operatorToVmPorts["source"]
	fmt.Println(sourceAddrs)
	for i := 0; i < len(sourceAddrs); i++ {
		args := &ArgsWithSender{
			Rt:        sourceTriggers[i],
			SenderNum: 0,
			SenderPort: SCHEDULER_PORT,
			TargetPort: sourceAddrs[i].port,
			UID: strconv.Itoa(i),
		}
		sendRequestToServer(sourceAddrs[i].VM, sourceAddrs[i].port, args)
	}
}

func genTopology(num_tasks int) [][]int {
	nodeTopology := make([][]int, 3, 3)

	// use successors (not member list) to determine number of members since rainstorm is just another hydfs command
	successorsMutex.RLock()

	for i := 0; i < len(successors)-1; i++ {
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

	for i := 0; i < len(successors)-1; i++ {
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

	for i := 0; i < len(successors)-1; i++ {
		if tasksPerWorker[successors[i]] > mostTasks {
			mostTasks = tasksPerWorker[successors[i]]
			mostTasksNode = successors[i]
		}
	}

	return mostTasksNode
}

func searchTopology(node int) []topology_entry_t {
	var matchingEntries []topology_entry_t
	for i := 0; i < len(topologyArray); i++ {
		for j := 0; j < len(topologyArray[i]); j++ {
			if topologyArray[i][j].VM == node {
				matchingEntries = append(matchingEntries, topology_entry_t{i, j})
			}
		}
	}
	return matchingEntries
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
	// TODO: merging on reschedules breaks program, see doc
	// if err := backgroundCommand(fmt.Sprintf("merge %s", hydfsSourceFile)); err != nil {
	// 	return "", err
	// }

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

func initRainstormOnScheduler(op1_exe string, op2_exe string, hydfs_src_file string, hydfs_dest_file string, num_tasks int) {
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
