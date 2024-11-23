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
	senderLayer := searchTopology(args.SenderNum).Layer
	if senderLayer == -1 {
		return fmt.Errorf("Error: node not found")
	}

	if senderLayer != RAINSTORM_LAYERS - 1 {
		keyHash := hash(args.Rt.Key, len(topologyArray[senderLayer + 1]))
		*reply = fmt.Sprintf("%d:%s", topologyArray[senderLayer + 1][keyHash].VM, topologyArray[senderLayer + 1][keyHash].port)
	}
	return nil
}
