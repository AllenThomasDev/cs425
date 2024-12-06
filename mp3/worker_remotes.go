package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type WorkerReq string

type ArgsWithSender struct {
	Rt        	Rainstorm_tuple_t
	SenderOp	string
	SenderHash	int
	TargetPort  string
	UID			string
}

type ReceiveAckArgs struct {
	Port string
	UID string
}

type KillTaskArgs struct {
	Port string
}

func startRPCListenerWorker(port string, endChannel chan bool) {
	workerreq := new(WorkerReq)
	rpc.Register(workerreq)
	servePort, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
  fmt.Printf("Started a server on port: %s\n", port)

  var wg sync.WaitGroup

	go func() {
		for {
			conn, err := servePort.Accept()
			if err != nil {
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				rpc.ServeConn(conn)
			}()
		}
	}()
	
	<-endChannel
	wg.Wait()
	fmt.Printf("Ending Rainstorm task on port %s\n", port)
	servePort.Close()
	cleanUpState(port, endChannel)
}

func (r *WorkerReq) HandleTuple(args *ArgsWithSender, reply *string) error {
	// this is data being sent from output stage, have scheduler write it to files
	if currentVM == LEADER_ID {
		fmt.Printf("%s:%s\n", args.Rt.Key, args.Rt.Value)
		// write data to output
		err := appendString(fmt.Sprintf("%s:%s\n", args.Rt.Key, args.Rt.Value), rainstormArgs.Hydfs_dest_file)
		if err != nil {
			return err
		}

		go sendAck(args)

	} else {
		opData, exists := portToOpData[args.TargetPort]
		if !exists {
			return fmt.Errorf("no data found for port %s", args.TargetPort)
		}

		senderAck := Ack_info_t{
			UID: args.UID,
			SenderOp: args.SenderOp,
			SenderHash: args.SenderHash,
		}
		
		if !screenInput(opData, senderAck) {
			return nil
		}

		opData.Input <- InputInfo{
			Tup : args.Rt,
			AckInfo: senderAck,
		}
	}

    return nil
}

func (r *WorkerReq) ReceiveAck(args *ReceiveAckArgs, reply *string) error {
	if currentVM == LEADER_ID {
		acksReceived++
		if acksReceived >= rainstormArgs.Num_tasks {
			fmt.Printf("Program has terminated, telling all tasks to stop...\n")
			endRainStorm <- true
		}
	} else {
		portToOpData[args.Port].RecvdAck <- args.UID
		rainstormLog.Printf("ReceiveACK: Received ACK on port %s with UID %s\n", args.Port, args.UID)
	}
	return nil
}

func sendAck (args *ArgsWithSender) {// immediately ACK sender
	senderAddr := operatorToVmPorts[args.SenderOp][args.SenderHash]
	receiverArgs := ReceiveAckArgs{
		Port: senderAddr.port,
		UID: args.UID,
	}
	err := sendAckToServer(senderAddr.VM, senderAddr.port, &receiverArgs)
	if err != nil {
		rainstormLog.Printf("error in immediate ack: %v\n", err)
	}
}

func (r *WorkerReq) KillTask (args *KillTaskArgs, reply *string) error {
	portToOpData[args.Port].Death <- true
	return nil
}

// func deferredStop (port string) {
// 	time.Sleep(time.Second)
// 	portToOpData[port].Death <- true
// }

func cleanUpState(port string, endChannel chan bool) {
	if currentVM != LEADER_ID {
		close(portToOpData[port].Input)
		close(portToOpData[port].RecvdAck)
		close(portToOpData[port].SendAck)
		delete(portToOpData, port)
	}
	close(endChannel)
}
