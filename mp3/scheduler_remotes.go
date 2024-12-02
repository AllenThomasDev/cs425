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
  fmt.Println("Someone is asking me where to put this shit, %s", args.Port)
	return nil
}
