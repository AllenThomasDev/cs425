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
	"strings"
)

var rainstormLog *log.Logger

type log_state_t struct {
	logFile string
	stateFile string
}

var (
	rainstormArgs   StartRainstormRemoteArgs // save args for rescheduling
	rainstormActive bool                     // flag to enable rescheduling on joins/leaves
	acksReceived	int 					// once leader has received num_tasks ACKs, we want to stop rainstorm and erase all state
	endRainStorm	chan bool
	endScheduler	chan bool
	endWorker		chan bool

	// this is vm:port:operator, needs to be updated when things fail
	currentActiveOperators map[int]map[string]int
	operatorToVmPorts map[string][]task_addr_t
	availableOperators []string
	operatorSequence []string
)

func rainstormMain(op1 string, op1_type Task_type_t, op1_args string, op2 string, op2_type Task_type_t, op2_args string, hydfs_src_file string, hydfs_dest_file string, numTasks int) {
	fmt.Println("Starting Rainstorm ...")
	// if valid := validateOperations([]string{op1, op2}); !valid {
	// 	return
	// }
	operatorSequence = make([]string, 3)
	operatorSequence = []string{"source", "op1", "op2"}
  rainstormActive = true
	rainstormArgs = StartRainstormRemoteArgs{
		op1,
		op1_type,
		op1_args,
		op2,
		op2_type,
		op2_args,
		hydfs_src_file,
		hydfs_dest_file,
		numTasks,
	}
	acksReceived = 0
	endRainStorm = make(chan bool)
	endScheduler = make(chan bool)
	endWorker 	= make(chan bool)

	currentActiveOperators = make(map[int]map[string]int)
	operatorToVmPorts = make(map[string][]task_addr_t)
	availableOperators = make([]string, 2)

	removeLogFiles(numTasks, operatorSequence)
	createLogFiles(numTasks, operatorSequence)
	backgroundCommand(fmt.Sprintf("createemptyfile %s", hydfs_dest_file))
	sourceArgs, err := createFileChunks(numTasks, hydfs_src_file)
	if err != nil {
		fmt.Printf("Error breaking file into chunks: %v\n", err)
		return
	}

	go startRPCListenerScheduler(endScheduler)
	go startRPCListenerWorker(CONSOLE_OUT_PORT, endWorker)
	
	distributeTasks(numTasks)
	rainstormLog.Println(operatorToVmPorts)
	err = initializeAllOperators()
	if err != nil {
		fmt.Printf("Error on operator initialization: %v\n", err)
		return
	}
	
	sourceTriggers := convertFileInfoStructListToTuples(hydfs_src_file, *sourceArgs, numTasks)
	startSources(sourceTriggers)
	rainstormLog.Println(currentActiveOperators, sourceTriggers)

	<-endRainStorm
	for op := range(operatorToVmPorts) {
		for i := 0; i < len(operatorToVmPorts[op]); i++ {
			killTask(operatorToVmPorts[op][i])
		}
	}
	endScheduler <- true
	endWorker <- true
	rainstormActive = false
}

func createLogFiles(numTasks int, operatorSequence []string) {
	for i := 0; i < len(operatorSequence); i++ {
		for j := 0; j < numTasks; j++ {
			backgroundCommand(fmt.Sprintf("createemptyfile %s_%d.log", operatorSequence[i], j))
			backgroundCommand(fmt.Sprintf("createemptyfile %s_%d_state.log", operatorSequence[i], j))
		}
	}
}

func removeLogFiles(numTasks int, operatorSequence []string) {
	for i := 0; i < len(operatorSequence); i++ {
		for j := 0; j < numTasks; j++ {
			backgroundCommand(fmt.Sprintf("remove %s_%d.log", operatorSequence[i], j))
			backgroundCommand(fmt.Sprintf("remove %s_%d_state.log", operatorSequence[i], j))
		}
	}
}

func callInitializeOperatorOnVM(vm int, port string, op string, t Task_type_t, exec string, hash int) error {
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
		OpType:		t,
		ExecName:	exec,
		Port:         port,
		LogFile: logs.logFile,
		StateFile: logs.stateFile,
		Hash:	hash,
		Numtasks: rainstormArgs.Num_tasks,
	}

	opNum := findLayerFromOperator(op)
	if opNum == -1 {
		return fmt.Errorf("Could not find layer from op name %s\n", op)
	}

	if opNum == 1 {
		opArgs.Args = rainstormArgs.Op1_args
	} else {
		opArgs.Args = rainstormArgs.Op1_args
	}
	
	var reply string
	err = client.Call("HyDFSReq.InitializeOperatorOnPort", opArgs, &reply)
	if err != nil {
		fmt.Printf("breaks here, %s", err.Error())
		return err
	}

	if currentActiveOperators[vm] == nil {
		currentActiveOperators[vm] = make(map[string]int)
	}

	currentActiveOperators[vm][port] = opNum
	rainstormLog.Printf("Started %s on VM %d:%s\n", op, vm, port)
	return nil
}

func initializeAllOperators() error {
	for opStr := range operatorSequence {
		for i := 0; i < len(operatorToVmPorts[operatorSequence[opStr]]); i++ {
			var t Task_type_t
			execStr := ""
			if opStr == 1 {
				execStr = rainstormArgs.Op1_exe
				t = rainstormArgs.Op1_type
			
			} else if opStr == 2 {
				execStr = rainstormArgs.Op2_exe
				t = rainstormArgs.Op2_type
			}
			err := callInitializeOperatorOnVM(operatorToVmPorts[operatorSequence[opStr]][i].VM, 
												operatorToVmPorts[operatorSequence[opStr]][i].port,
												operatorSequence[opStr],
												t,
												execStr,
												i)
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
  var tasksToBeResurrected []int
  for port, _ := range(tasksOnDeadVM) {
    tasksToBeResurrected = append(tasksToBeResurrected, tasksOnDeadVM[port])
    taskAddrToBeDeleted = append(taskAddrToBeDeleted, task_addr_t{vm, port})
  }
  for i := range(tasksToBeResurrected){
    // remove address from operatorToVmPorts[]
    operatorName := operatorSequence[tasksToBeResurrected[i]]
	
	for {
		destination := findNodeWithFewestTasks()
		port, err := callFindFreePort(destination)
		if err != nil {
			continue
		}
		newTaskAddr := task_addr_t{
			VM: destination,
			port: port,
		}
	
		newHash := modifyOperator(operatorName, taskAddrToBeDeleted[i], newTaskAddr)
		fmt.Printf("Rescheduling operator %s hash %d\n", operatorName, newHash)
		if strings.Contains(operatorName, "1") {
			err = callInitializeOperatorOnVM(destination, port, operatorName, rainstormArgs.Op1_type, rainstormArgs.Op1_exe, newHash)
		} else {
			err = callInitializeOperatorOnVM(destination, port, operatorName, rainstormArgs.Op2_type, rainstormArgs.Op2_exe, newHash)
		}

		if err == nil {
			break
		} else {
			fmt.Printf("Error on rebalance: %v\n", err)
		}
	}
  }
}

func findLayerFromOperator(operatorName string) int {
	for i := 0; i < len(operatorSequence); i++ {
		if operatorSequence[i] == operatorName {
			return i
		}
	}
	return -1
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

func killTask (Addr task_addr_t) {
	// Establish a connection to the RPC server
	fmt.Printf("Killing task on node %d port %s\n", Addr.VM, Addr.port)
	client, err := rpc.Dial("tcp", vmToIP(Addr.VM)+":"+Addr.port)
	if err != nil {
		rainstormLog.Printf("I WANT TO LIIIIIIIIIVE: %v\n", err)
	}
	defer client.Close()
	var reply string
	err = client.Call("WorkerReq.KillTask", KillTaskArgs{Addr.port}, &reply)
	if err != nil {
		rainstormLog.Println("Error during RPC call:", err)
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

func getGrinderNode() int {
	var grinder int
	maxTasks := -1

	for key, operatorMap := range currentActiveOperators {
		length := len(operatorMap)
		if maxTasks == -1 || length > maxTasks {
			maxTasks = length
			grinder = key
		}
	}

	if maxTasks <= 1 {
		return -1 // return -1 if all nodes have <= 1 task; no sense swiping a node's only task
	}

	return grinder
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
			lineCount++
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
	rainstormLog.Printf("Distributing lines: lineCount = %d, numSources = %d\n", lineCount, numSources)
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

func initRainstormOnScheduler(op1_exe string, op1_type string, op1_args string, op2_exe string, op2_type string, op2_args string, hydfs_src_file string, hydfs_dest_file string, num_tasks int) {
	op1_type_num, err := strconv.Atoi(op1_type)
	if err != nil || op1_type_num > 2 || op1_type_num < 0 {
		fmt.Printf("Error: op type must be 0 (stateless), 1 (stateful) or 2 (filter\n)")
		return
	}
	one_type := Task_type_t(op1_type_num)
	fmt.Printf("one_type: %v\n", one_type)

	op2_type_num, err := strconv.Atoi(op2_type)
	if err != nil || op1_type_num > 2 || op1_type_num < 0 {
		fmt.Printf("Error: op type must be 0 (stateless), 1 (stateful) or 2 (filter\n)")
		return
	}
	two_type := Task_type_t(op2_type_num)
	fmt.Printf("two_type: %v\n", two_type)
	
	if selfIP == introducerIP {
		rainstormMain(op1_exe, one_type, op1_args, op2_exe, two_type, op2_args, hydfs_src_file, hydfs_dest_file, num_tasks)
	} else {
		client, err := rpc.DialHTTP("tcp", introducerIP+":"+RPC_PORT)
		if err != nil {
			fmt.Printf("Failed to dial introducer: %v\n", err)
			return
		}

		var reply string
		err = client.Call("HyDFSReq.StartRainstormRemote", StartRainstormRemoteArgs{op1_exe, one_type, op1_args, op2_exe, two_type, op2_args, hydfs_src_file, hydfs_dest_file, num_tasks}, &reply)
		if err != nil {
			fmt.Printf("Failed to initiate Rainstorm: %v\n", err)
		}
	}
}
