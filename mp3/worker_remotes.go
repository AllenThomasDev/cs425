package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

type WorkerReq string

func startRPCListenerWorker(port int) {
	workerreq := new(WorkerReq)
	rpc.Register(workerreq)
	servePort, err := net.Listen("tcp", ":" + strconv.Itoa(port))
	if err != nil {
		panic(err)
	}
	go http.Serve(servePort, nil)
}

// used by scheduler to tell worker to stop task
func (w *WorkerReq) KillSelf(reply *string) error {
	fmt.Println("I have been told to kill myself :(")
	return nil
}