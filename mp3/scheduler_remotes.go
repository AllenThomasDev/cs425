package main

import (
	"fmt"
	"net"
	"net/rpc"
)

type SchedulerReq string

func startRPCListenerScheduler() {
	schedulerreq := new(SchedulerReq)
	rpc.Register(schedulerreq)
	servePort, err := net.Listen("tcp", ":" + SCHEDULER_PORT)
	if err != nil {
		panic(err)
	}
	go rpc.Accept(servePort)
}

func (s *SchedulerReq) GetNextStage(args *ArgsWithSender, reply *string) error {
	// Locate tasks for the sender node in the topology
  fmt.Print("I;m in GetNextStage\n here the map looks like - \n \n")
  showTopology()
	senderTasks := searchTopology(args.SenderNum)
  //////////////// i need a to fix this, thsi shouyld be easier
	if senderTasks == nil {
    showTopology()
    fmt.Printf("%d, %s",args.SenderNum, args.Port)
		return fmt.Errorf("Error: node not found")
	}

	for i := 0; i < len(senderTasks); i++ {
		// Check if the current task matches the sender's port
		if topologyArray[senderTasks[i].Layer][senderTasks[i].Hash].port == args.Port {
			// Hash the tuple's key to determine the next node in the next layer
			keyHash := hash(args.Rt.Key, len(topologyArray[senderTasks[i].Layer + 1]))
			// Construct the address of the next stage as "VM:port"
			retStr := fmt.Sprintf("%d:%s", topologyArray[senderTasks[i].Layer + 1][keyHash].VM, topologyArray[senderTasks[i].Layer + 1][keyHash].port)
			fmt.Println(retStr)
			// Set the reply to the target node's address
			*reply = fmt.Sprintf(retStr)
		}
	}
	return nil
}
