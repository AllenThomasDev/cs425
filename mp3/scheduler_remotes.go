package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
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
	} else if senderLayer == 2 {
		// to indicate sender is in the last stage and should write to console/output file, send them back their number
		*reply = strconv.Itoa(args.SenderNum)
	}
	keyHash := hash(args.Rt.Key, len(topologyArray[senderLayer + 1]))
	*reply = strconv.Itoa(topologyArray[senderLayer + 1][keyHash])
	return nil
}
