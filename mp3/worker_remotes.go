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
	Rt        	Rainstorm_tuple_t
	SenderNum	int
	SenderPort	string
	TargetPort  string
	UID			string
}

type ReceiveAckArgs struct {
	AckInfo	Ack_info_t
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
	// this is data being sent from output stage, have scheduler write it to files
	if ipToVM(selfIP) == LEADER_ID {
		// write data to output
		err := backgroundCommand(fmt.Sprintf("appendstring %s:%s %s", args.Rt.Key, args.Rt.Value, rainstormArgs.Hydfs_dest_file))
		if err != nil {
			return err
		}

		go sendAck(args)

	} else {
		channels, exists := portToChannels[args.TargetPort]
		if !exists {
			return fmt.Errorf("no channels found for port %s", args.TargetPort)
		}
		
		senderAck := Ack_info_t{
			UID: args.UID,
			SenderNum: args.SenderNum,
			SenderPort: args.SenderPort,
		}

		channels.Input <- InputInfo{
			Tup : args.Rt,
			AckInfo: senderAck,
		}
	}

    return nil
}

func (r *WorkerReq) ReceiveAck(args *ReceiveAckArgs, reply *string) error {
	portToChannels[args.AckInfo.SenderPort].RecvdAck <- args.AckInfo
	fmt.Printf("ReceiveACK: Received ACK on port %s with UID %s\n", args.AckInfo.SenderPort, args.AckInfo.UID)
	return nil
}

func sendAck (args *ArgsWithSender) {// call ACK on sender
	err := callReceiveAck( Ack_info_t{
		UID: args.UID,
		SenderNum: args.SenderNum,
		SenderPort: args.SenderPort,
	});
	if err != nil {
		fmt.Printf("error in immediate ack: %v\n", err)
	}
}
