package main

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func copyFile(sourcePath, destinationPath string) error {
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()
	destFile, err := os.Create(destinationPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destFile.Close()
	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file contents: %w", err)
	}
	err = destFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync destination file: %w", err)
	}
	return nil
}

func mockProcessRecord(uniqueID int, line string, hydfsSrcFile string, logFile string, oldLogFile string, port string) {
	if isProcessed(uniqueID, oldLogFile) {
		return
	}
	key := hydfsSrcFile + ":" + strconv.Itoa(uniqueID)
	args := ArgsWithSender{Rainstorm_tuple_t{key, line}, currentVM, port}
	sendToNextStage(args)
	fmt.Printf("here hereh here\n")
	logProcessed(uniqueID, logFile)
}

func mockBackgroundCommand(command string) error {
	parts := strings.Split(command, " ")
	copyFile("client/"+parts[1], "client/"+parts[2])
	return nil
}

func mockLogProcessed(uniqueID int, logFile string) {
	fmt.Printf("Logged uniqueID %d to file %s\n", uniqueID, logFile)
}

func mockRandomFileName() string {
	return "test_random_file_name.txt"
}


func MockSendRequestToServer(port string, args *ArgsWithSender) {
	// Establish a connection to the RPC server
	client, err := rpc.Dial("tcp", "localhost:"+port)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer client.Close()
	var reply string
	err = client.Call("WorkerReq.HandleTuple", args, &reply)
	if err != nil {
		fmt.Println("Error during RPC call:", err)
		return
	}
	fmt.Println("Server reply:", reply)
}

func TestRPCCommunication(t *testing.T) {
	port := "12345"
	go startRPCListenerWorker(port)
  go startRPCListenerScheduler()
  initOperators()
  callInitializeOperatorOnVM(1, operators["splitLineOperator"])
	time.Sleep(6 * time.Second)
	fmt.Println("done waiting")
	args := &ArgsWithSender{
		Rt:        Rainstorm_tuple_t{"1", "1"}, // Use appropriate data
		SenderNum: 1,
		Port:      port,
	}
	sendRequestToServer(0, port, args)
}
