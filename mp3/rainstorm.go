package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

var rainstormLog *log.Logger

// TODO: Have operators check logfile when processing inputs, make program actually conclude, test wordcount on failures

type log_state_t struct {
	logFile string
	stateFile string
}

// this is vm:port:operator, needs to be updated when things fail
var currentActiveOperators = make(map[int]map[string]Operator)
var operatorToVmPorts = make(map[string][]task_addr_t)
var availableOperators = make([]string, 2)
var operatorSequence = make([]string, 3)

var (
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
	// TODO: every time that membership list is updated, we need to update a lot of globals
	createLogFiles(numTasks, operatorSequence)
	backgroundCommand(fmt.Sprintf("createemptyfile %s", hydfs_dest_file))
	// here i am making the assumption that a source does not fail before we send the chunks to it
	sourceArgs, err := createFileChunks(numTasks, hydfs_src_file)
	if err != nil {
		fmt.Printf("Error breaking file into chunks: %v\n", err)
		return
	}

	go startRPCListenerScheduler()
	go startRPCListenerWorker(CONSOLE_OUT_PORT)
	
	distributeTasks(numTasks)
	err = initializeAllOperators()
	if err != nil {
		fmt.Printf("Error on operator initialization: %v\n", err)
		return
	}
	
	sourceTriggers := convertFileInfoStructListToTuples(hydfs_src_file, *sourceArgs, numTasks)
	startSources(sourceTriggers)
	fmt.Println(currentActiveOperators, sourceTriggers)
	select {}
}

func createLogFiles(numTasks int, operatorSequence []string) {
	for i := 0; i < len(operatorSequence); i++ {
		for j := 0; j < numTasks; j++ {
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

func callInitializeOperatorOnVM(vm int, port string, op string, hash int) error {
	client, err := rpc.DialHTTP("tcp", vmToIP(vm)+":"+RPC_PORT)
	if err != nil {
		fmt.Printf("breaks here, %s", err.Error())
		return err
	}

	logs, err := getTaskLog(vm, port)
	if err != nil {
		return err
	}
	
	opArgs := InitializeOperatorArgs{
		OperatorName: op,
		Port:         port,
		LogFile: logs.logFile,
		StateFile: logs.stateFile,
		Hash:	hash,
		Numtasks: rainstormArgs.Num_tasks,
	}
	
	var reply string
	err = client.Call("HyDFSReq.InitializeOperatorOnPort", opArgs, &reply)
	if err != nil {
		fmt.Printf("breaks here, %s", err.Error())
		return err
	}

	if currentActiveOperators[vm] == nil {
		currentActiveOperators[vm] = make(map[string]Operator)
	}
	currentActiveOperators[vm][port] = operators[op]
	rainstormLog.Printf("Started %s on VM %d:%s\n", operators[op].Name, vm, port)
	return nil
}

func initializeAllOperators() error {
	for opStr := range operatorSequence {
		for i := 0; i < len(operatorToVmPorts[operatorSequence[opStr]]); i++ {
			
			err := callInitializeOperatorOnVM(operatorToVmPorts[operatorSequence[opStr]][i].VM, operatorToVmPorts[operatorSequence[opStr]][i].port, operatorSequence[opStr], i)
			if err != nil {
				return err
			}
		}
	}
	return nil
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
	destination := findNodeWithFewestTasks()
	port, err := callFindFreePort(destination)
	if err != nil {
		panic(err) // we're screwed if we can't find a free port on the new machine
	}

	newTaskAddr := task_addr_t{
		VM: destination,
		port: port,
	}

    newHash := modifyOperator(operatorName, taskAddrToBeDeleted[i], newTaskAddr)
	callInitializeOperatorOnVM(destination, port, operatorName, newHash)
  }
}

func modifyOperator(operatorName string, delAddr task_addr_t, newAddr task_addr_t) int {
	// Retrieve the slice of task addresses for the given operator
	taskAddrsForOperators := operatorToVmPorts[operatorName]
	// Iterate through the slice to find the address and remove it
	var i int
	for i = 0; i < len(taskAddrsForOperators); i++ {
	  if taskAddrsForOperators[i] == delAddr {
		// Shift all elements to the left, effectively removing the element at index i
		taskAddrsForOperators[i] = newAddr
		break // Exit after removing the address
	  }
	}
	//entries are added in this global when a task is initalized
	operatorToVmPorts[operatorName] = taskAddrsForOperators
	rainstormLog.Print("things were updated\n")
	rainstormLog.Print(operatorToVmPorts[operatorName])
	return i
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
  rainstormLog.Print("things were updated\n")
  rainstormLog.Print(operatorToVmPorts[operatorName])
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
		port, err := callFindFreePort(member)
		if err != nil {
			panic(err) // system will definitely break if this fails, so panic
		}

		operatorToVmPorts[operator] = append(operatorToVmPorts[operator], task_addr_t{member, port})
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
	log.Panicf("couldn't find operator for %d:%s\n", taskAddr.VM, taskAddr.port)
	return ""
	// TODO:
	// we could have problems if something dies, we remove it from the operator
	// to VMs map and then we receive a tuple, we won't know where we got it from
	// ,thus maybe we should have 2 maps, one that has all operators and ports
	// mappings that were ever creaeted, the other has only active ones
	// this can be solved if we along with the tuple, we just send the operator that just operated on the tuple, instead of the VM:Port
	// but i am bit tired to make that refactor right now
}

func matchTaskWithHash(taskAddr task_addr_t, opString string) int {
	for i := 0; i < len(operatorToVmPorts[opString]); i++ {
		if operatorToVmPorts[opString][i] == taskAddr {
			return i
		}
	}
	return -1
}

func getTaskLog(vm int, port string) (log_state_t, error) {
	var ls log_state_t
	
	taskAddr := task_addr_t{vm, port}
	taskOp := findOperatorFromTaskAddr(taskAddr)
	currHash := matchTaskWithHash(taskAddr, taskOp)
	if currHash == -1 {
		return ls, fmt.Errorf("Error: task not found in operator to port mappings, state may be outdated\n")
	}
	
	logPrefix := taskOp + "_" + strconv.Itoa(currHash)
	ls.logFile = logPrefix + ".log"
	ls.stateFile = logPrefix + "_state" + ".log"
	return ls, nil
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
			SenderOp:	"leader",
			TargetPort: sourceAddrs[i].port,
			UID: strconv.Itoa(i),
		}
		sendRequestToServer(sourceAddrs[i].VM, sourceAddrs[i].port, args)
	}
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
