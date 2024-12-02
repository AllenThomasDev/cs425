package main

import (
	"fmt"
	"net"
	"net/rpc"
)

type SchedulerReq string

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

func (s *SchedulerReq) GetNextStage(args *ArgsWithSender, reply *string) error {
  incomingTupleAddr := task_addr_t{args.SenderNum, args.Port}
  incomingOperator := findOperatorFromTaskAddr(incomingTupleAddr) 
  nextOperator := getNextOperator(incomingOperator)
  keyHash := hash(args.Rt.Key, len(operatorToVmPorts[nextOperator]))
  nextStageTaskAddr := operatorToVmPorts[nextOperator][keyHash]
  retStr := fmt.Sprintf("%d:%s", nextStageTaskAddr.VM, nextStageTaskAddr.port)
  *reply = retStr
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
