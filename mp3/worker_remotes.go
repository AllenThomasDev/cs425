package main

import (
	"fmt"
	"net"
	"net/rpc"
)

type WorkerReq string

type StopTaskArgs struct {
	Port string
}

type ArgsWithSender struct {
	Rt        Rainstorm_tuple_t
	SenderNum int
	Port      string
}

func startRPCListenerWorker(port string) {
	workerreq := new(WorkerReq)
	rpc.Register(workerreq)
	servePort, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
  fmt.Printf("Started a server on port: %s", port)
	go rpc.Accept(servePort)
}

func (w *WorkerReq) StopTask(args *StopTaskArgs, reply *string) error {
	go deferredStop(args.Port)
	return nil
}

func (w *WorkerReq) RunExec(reply *string) error {
	// run executable, send to buffer where we will attempt to send to subsequent stages
	return nil
}

func (r *WorkerReq) HandleTuple(args *ArgsWithSender, reply *string) error {
    channels, exists := portToChannels[args.Port]
    if !exists {
        return fmt.Errorf("no channels found for port %s", args.Port)
    }
    channels.Input <- args.Rt
    return nil
}
