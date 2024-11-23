package main

import (
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
	rpc.HandleHTTP()
	servePort, err := net.Listen("tcp", ":" + SCHEDULER_PORT)
	if err != nil {
		panic(err)
	}
	go http.Serve(servePort, nil)
}

// func (s *SchedulerReq) GetNextStage(args *GetNextStageArgs, reply *string) error {
// 	senderLayer := searchTopology(args.SenderNum).layer
// 	if senderLayer == -1 {
// 		return fmt.Errorf("Error: node not found")
// 	}
// 	strconv.Atoi()
// }
