package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
)

type WorkerReq string

type StopTaskArgs struct {
	Port string
}

func startRPCListenerWorker(port string) {
	workerreq := new(WorkerReq)
	rpc.Register(workerreq)
	servePort, err := net.Listen("tcp", ":" + port)
	if err != nil {
		panic(err)
	}
	go http.Serve(servePort, nil)
	
	<-stopChannels[port]

	fmt.Println("closing channel")
	servePort.Close()
}

// used by scheduler to tell worker to stop task
func (w *WorkerReq) StopTask(args *StopTaskArgs, reply *string) error {
	go deferredStop(args.Port)
	return nil
}

func (w *WorkerReq) RunExec(reply *string) error {
	// run executable, send to buffer where we will attempt to send to subsequent stages
	return nil
}