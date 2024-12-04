package main

import (
	"fmt"
	"net"
	"net/rpc"
	"strconv"
)

type SchedulerReq string

type GetTaskLogArgs struct {
	VM int
	Port string
}
type GetNextStageArgs struct {
	VM int
	Port string
	Rt Rainstorm_tuple_t
}

func startRPCListenerScheduler() (net.Listener, error) {
	schedulerreq := new(SchedulerReq)
	rpc.Register(schedulerreq)
	servePort, err := net.Listen("tcp", ":" + SCHEDULER_PORT)
	if err != nil {
		panic(err)
	}
	go rpc.Accept(servePort)
	return servePort, nil
}

func stopRPCListener(listener net.Listener) {
	if listener != nil {
		err := listener.Close()
		if err != nil {
			fmt.Printf("Error closing listener: %v\n", err)
		} else {
			fmt.Println("Listener closed successfully.")
		}
	}
}

func (s *SchedulerReq) GetNextStage(args *GetNextStageArgs, reply *string) error {
  incomingTupleAddr := task_addr_t{args.VM, args.Port}
  incomingOperator := findOperatorFromTaskAddr(incomingTupleAddr) 
  nextOperator := getNextOperator(incomingOperator)
  // if operation has been completed, have data get sent to the leader
  if nextOperator == "completed" {
	*reply = fmt.Sprintf("%d:%s", LEADER_ID, CONSOLE_OUT_PORT)
  } else {
	keyHash := hash(args.Rt.Key, len(operatorToVmPorts[nextOperator]))
	nextStageTaskAddr := operatorToVmPorts[nextOperator][keyHash]
	retStr := fmt.Sprintf("%d:%s", nextStageTaskAddr.VM, nextStageTaskAddr.port)
	*reply = retStr
  }
  return nil
}

func (s *SchedulerReq) GetTaskLog(args *GetTaskLogArgs, reply *string) error {
	incomingTupleAddr := task_addr_t{args.VM, args.Port}
	incomingOperator := findOperatorFromTaskAddr(incomingTupleAddr)
	currHash := matchTaskWithHash(incomingTupleAddr, incomingOperator)
	if currHash == -1 {
		return fmt.Errorf("Error: task not found in operator to port mappings, state may be outdated\n")
	}
	logPrefix := incomingOperator + "_" + strconv.Itoa(currHash)
	*reply = fmt.Sprintf("%s:%s", logPrefix + ".log", logPrefix + "_state" + ".log")
	return nil
}


	// for i := 0; i < len(senderTasks); i++ {
	// 	// Check if the current task matches the sender's port
	// 	if topologyArray[senderTasks[i].Layer][senderTasks[i].Hash].port == args.Port {
	// 		// Hash the tuple's key to determine the next node in the next layer
	// 		keyHash := hash(args.Rt.Key, len(topologyArray[senderTasks[i].Layer + 1]))
	// 		// Construct the address of the next stage as "VM:port"
	// 		retStr := fmt.Sprintf("%d:%s", topologyArray[senderTasks[i].Layer + 1][keyHash].VM, topologyArray[senderTasks[i].Layer + 1][keyHash].port)
	// 		fmt.Println(retStr)
	// 		// Set the reply to the target node's address
	// 		*reply = fmt.Sprintf(retStr)
	// 	}
	// }
	//
