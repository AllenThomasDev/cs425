package main

import (
	"fmt"
	"net"
	"net/rpc"
)

type SchedulerReq string

type GetNextStageArgs struct {
	CurrOperator	string
	Port			string
	Rt Rainstorm_tuple_t
}

type GetPrevStageArgs struct {
	PrevOperator string
	PrevHash int
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
  nextOperator := getNextOperator(args.CurrOperator)
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

func (s *SchedulerReq) GetPrevStage(args *GetPrevStageArgs, reply *string) error {
	if (args.PrevOperator == "leader") {
		*reply = fmt.Sprintf("%d:%s", LEADER_ID, CONSOLE_OUT_PORT)
	} else {
		prevStageTaskAddr := operatorToVmPorts[args.PrevOperator][args.PrevHash]
	  	retStr := fmt.Sprintf("%d:%s", prevStageTaskAddr.VM, prevStageTaskAddr.port)
	  	*reply = retStr
	}
	return nil
  }
