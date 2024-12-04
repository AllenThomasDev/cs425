package main

import (
	"fmt"
	"net/rpc"
	"strconv"
	"strings"
	"time"
)

var (
	stopChannels =  make(map[string] chan string)
)

// listens to input channel, processes the tuple using opeartor and puts it to output channel

func processInputChannel(operatorName string, channels OperatorChannels, port string) {
	for input := range channels.Input {
		// TODO: check logfile here
		output := operators[operatorName].Operator(input.Tup)
		opsDone := 0
		switch v := output.(type) {
		case chan Rainstorm_tuple_t:
			fmt.Printf("I'm processing a channel!\n")
			emptyChannel := true
			// keep a one-value buffer here so we know which value is last
			bufTup := Rainstorm_tuple_t{"I AM UNINITIALIZED", "SOOOOO UNINITIALIZED"}
			for tuple := range v {
				emptyChannel = false
				if bufTup.Key != "I AM UNINITIALIZED" && bufTup.Value != "SOOOOO UNINITIALIZED" {
					fmt.Printf("Writing %s:%s to output\n", bufTup.Key, bufTup.Value)
					channels.Output <- OutputInfo{
						Tup: bufTup,
						UID: input.AckInfo.UID + ":" + strconv.Itoa(opsDone),
						AckInfo: input.AckInfo,
					}
					channels.SendAck <- false
				}
				bufTup = tuple
				opsDone++
			}

			// channel has closed, send last value and signal that we should send out an ack
			if emptyChannel == false {
				fmt.Printf("channel is not empty!\n")
			} else {
				fmt.Printf("channel is empty??\n")
			}

			if emptyChannel == false {
				fmt.Printf("Writing %s:%s (final tuple) to output\n", bufTup.Key, bufTup.Value)
				channels.Output <- OutputInfo{
					Tup: bufTup,
					UID: input.AckInfo.UID + ":" + strconv.Itoa(opsDone),
					AckInfo: input.AckInfo,
				}
				fmt.Printf("Channel closed, now we can ACK!\n")
				channels.SendAck <- true
			} else {
				// send from here if our channel was full of nothin'
				err := sendAckFromInput(input.AckInfo)
				if err != nil {
					fmt.Printf("Error on empty channel ACK: %v\n", err)
				}
			}

		case Rainstorm_tuple_t:
			fmt.Printf("I'm processing a single tuple!\n")
			channels.Output <- OutputInfo{
				Tup: v,
				UID: input.AckInfo.UID + ":" + strconv.Itoa(opsDone),
				AckInfo: input.AckInfo,
			}
		  	// send ack after each output has been ACKed
		  	channels.SendAck <- true
		case []Rainstorm_tuple_t:
			fmt.Printf("I'm processing a slice of tuples!\n")
			for i := 0; i < len(v); i++ {
				channels.Output <- OutputInfo{
					Tup: v[i],
					UID: input.AckInfo.UID + ":" + strconv.Itoa(opsDone),
					AckInfo: input.AckInfo,
				}
				opsDone++
				if i == len(v) - 1 {
					// send ack after last tuple in slice has been ACKed
					channels.SendAck <- true
				} else {
					channels.SendAck <- false
				}
			}
		default:
		  fmt.Printf("Unexpected output type: %T\n", output)
		}
	  }
	close(channels.Output) // Close the output channel when input channel is closed
} 
  
// Function to print the contents of the output channel
func processOutputChannel(operatorName string, channels OperatorChannels, port string) {
	for out := range channels.Output {
		fmt.Printf("ProcessOutput: Trying to process output: %s:%s\n", out.Tup.Key, out.Tup.Value)
		for {
			nextStageArgs := GetNextStageArgs{
				Rt: out.Tup,
				VM: ipToVM(selfIP),
				Port: port,
			}
			
			err := sendToNextStage(nextStageArgs, out.UID)
			if err != nil {
				fmt.Printf("Error on next stage send: %v\n", err)
				time.Sleep(RAINSTORM_ACK_TIMEOUT)
			} else {
				err = handleAcks(operatorName, channels, out)
				if err != nil {
					fmt.Printf("Error on ACK handling: %v\n", err)
					time.Sleep(RAINSTORM_ACK_TIMEOUT)
				} else {
					break
				}
			}
			fmt.Printf("Transmission failed or ack timed out, trying again...\n")
		}

		fmt.Printf("Output iteration finished\n")
	}
}

func handleAcks(operatorName string, channels OperatorChannels, out OutputInfo) error {
	select {
	case ackInfo := <- channels.RecvdAck:
		logFiles := getTaskLogFromScheduler(&GetTaskLogArgs{ackInfo.SenderNum, ackInfo.SenderPort})
		logFilesParts := strings.Split(logFiles, ":")
		logFile := logFilesParts[0]
		stateFile := logFilesParts[1]

		backgroundWrite(ackInfo.UID, logFile)
		if operators[operatorName].Stateful {
			backgroundWrite(fmt.Sprintf("%s:%s\n", out.Tup.Key, out.Tup.Value), stateFile)
		}

		var err error

		fmt.Printf("trying to send ack now...\n")
		if <- channels.SendAck {
			fmt.Printf("Sending ACK with ID %s to %d:%s\n", out.AckInfo.UID, out.AckInfo.SenderNum, out.AckInfo.SenderPort)
			err = callReceiveAck(out.AckInfo)
		}
		fmt.Printf("ack channel cleared\n")
		return err
	case <-time.After(RAINSTORM_ACK_TIMEOUT):
		return fmt.Errorf("timeout!")
	}
}

func sendAckFromInput(ack Ack_info_t) error {
	fmt.Printf("Sending ACK with ID %s to %d:%s\n", ack.UID, ack.SenderNum, ack.SenderPort)
	return callReceiveAck(ack)
}

func callReceiveAck(ackInfo Ack_info_t) error {
	client, err := rpc.Dial("tcp", vmToIP(ackInfo.SenderNum) + ":" + ackInfo.SenderPort)
	if err != nil {
		return err
	}
	defer client.Close()
	var reply string
	ackArgs := ReceiveAckArgs{
		AckInfo: ackInfo,
	}
	
	err = client.Call("WorkerReq.ReceiveAck", ackArgs, &reply)
	if err != nil {
		return err
	}
	return nil
}

func deferredStop(port string) {
	fmt.Println("About to die...")
	time.Sleep(time.Millisecond)
	// need to send TWO stop requests, one for RPC listener and one for function wrapper
	stopChannels[port] <- "die."
	stopChannels[port] <- "die."
}