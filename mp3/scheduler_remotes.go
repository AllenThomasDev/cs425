package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
)

type SchedulerReq string

type GetNextStageArgs struct {
	Rt Rainstorm_tuple_t
	SenderNum int
	Port string
}

func startRPCListenerScheduler() {
	schedulerreq := new(SchedulerReq)
	rpc.Register(schedulerreq)
	servePort, err := net.Listen("tcp", ":" + SCHEDULER_PORT)
	if err != nil {
		panic(err)
	}
	go http.Serve(servePort, nil)
}

func (s *SchedulerReq) GetNextStage(args *GetNextStageArgs, reply *string) error {
	senderTasks := searchTopology(args.SenderNum)
	if senderTasks == nil {
		return fmt.Errorf("Error: node not found")
	}

	for i := 0; i < len(senderTasks); i++ {
		if topologyArray[senderTasks[i].Layer][senderTasks[i].Hash].port == args.Port {
			keyHash := hash(args.Rt.Key, len(topologyArray[senderTasks[i].Layer + 1]))
			retStr := fmt.Sprintf("%d:%s", topologyArray[senderTasks[i].Layer + 1][keyHash].VM, topologyArray[senderTasks[i].Layer + 1][keyHash].port)
			fmt.Println(retStr)
			*reply = fmt.Sprintf(retStr)
		}
	}
	return nil
}
