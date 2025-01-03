package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
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

func startRPCListenerScheduler(schChannel chan bool) {
	schedulerreq := new(SchedulerReq)
	rpc.Register(schedulerreq)
	servePort, err := net.Listen("tcp", ":" + SCHEDULER_PORT)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	go func() {
		for {
			conn, err := servePort.Accept()
			if err != nil {
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				rpc.ServeConn(conn)
			}()
		}
	}()
	
	<-schChannel
	wg.Wait()
	fmt.Printf("Shutting down RainStorm Scheduler\n")
	servePort.Close()
	close(schChannel)
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
