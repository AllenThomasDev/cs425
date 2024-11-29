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
	go rpc.Accept(servePort)

	<-stopChannels[port]

	fmt.Println("closing channel")
	servePort.Close()
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
	// Process the incoming request
	fmt.Printf("Received request from sender %d, Port: %s\n", args.SenderNum, args.Port)
	fmt.Printf("Rainstorm tuple: %s\n", args.Rt)
	*reply = fmt.Sprintf("Request from sender %d processed successfully", args.SenderNum)
	return nil
}
