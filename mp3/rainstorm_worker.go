package main

import (
	"fmt"
	"strconv"
	"time"
)

// check if new data has already been received, keeping it out of input channel if it has
func screenInput(opData OperatorData, AckInfo Ack_info_t) bool {	
	opData.UIDBufLock.Lock()
	defer opData.UIDBufLock.Unlock()

	bufLocal := *opData.UIDBuf

	var i int
	for i = 0; i < len(bufLocal); i++ {
		// entries are popped UIDBuf starting at index 0, so if we've encountered an empty slot, our data isn't present in the buffer
		if (bufLocal[i] == EMPTY) {
			break
		}

		// if the data is in the buffer, we will send out an ack at some point in the future, so just drop this data
		if (bufLocal[i] == AckInfo.UID) {
			rainstormLog.Printf("Data found in buffer, discarding...\n")
			return false
		}
	}

	processed, err := checkLogFile(AckInfo.UID, opData.LogFile)
	if err != nil {
		rainstormLog.Printf("Error checking logfile\n")
		panic(err)
	}
	if processed {
		// if the data was found in the log, we don't want to send data to the next stage, but we MUST send an ack to the previous stage
		rainstormLog.Printf("Data found in log file, sending ack to previous stage...\n")
		// use a new goroutine here since screenInput is called from an RPC the sender of the data is currently waiting for a return from
		go ackPrevStage(AckInfo)
		return false
	} 

	// if the new data wasn't in our buffer or log file, add it to the buffer
	bufLocal[i] = AckInfo.UID

	for i := 0; i < len(bufLocal); i++ {
		rainstormLog.Printf("Screen: UID at index %d: %s\n", i, bufLocal[i])
	}

	*opData.UIDBuf = bufLocal

	return true
}

// listens to input channel, processes the tuple using opeartor and puts it to output channel
func processInputChannel(opData OperatorData, port string) {
	for input := range opData.Input {

		var output interface{}
		if operators[opData.Op].Filter {
			output = operators[opData.Op].Operator(FilterArgs{input.Tup, "Streetname"})
		} else if operators[opData.Op].Stateful {
			output = operators[opData.Op].Operator(StatefulArgs{input.Tup, port})
		} else {
			output = operators[opData.Op].Operator(StatelessArgs{input.Tup})
		}
		
		opsDone := 0 // counter for UIDs
		switch v := output.(type) {
		case chan Rainstorm_tuple_t:
			rainstormLog.Printf("Processing a channel!\n")
			emptyChannel := true
			// since we only know processing is finished when a channel has closed, we need to maintain a one-value buffer,
			// otherwise output processing may not ack after the last value has been sent out and acked
			bufTup := Rainstorm_tuple_t{MAGIC_STR1, MAGIC_STR2}
			bufUID := ""

			for tuple := range v {
				emptyChannel = false
				if bufTup.Key != MAGIC_STR1 && bufTup.Value != MAGIC_STR2 {
					rainstormLog.Printf("Writing %s:%s to output\n", bufTup.Key, bufTup.Value)
					opData.Output <- OutputInfo{
						Tup: bufTup,
						UID: bufUID,
						AckInfo: input.AckInfo,
					}
					opData.SendAck <- false
				}
				bufTup = tuple
				bufUID = input.AckInfo.UID + ":" + strconv.Itoa(opsDone)
				opsDone++
			}

			// channel has closed, send last value and signal that we should send out an ack
			if emptyChannel == false {
				rainstormLog.Printf("Writing %s:%s (final tuple) to output\n", bufTup.Key, bufTup.Value)
				opData.Output <- OutputInfo{
					Tup: bufTup,
					UID: bufUID,
					AckInfo: input.AckInfo,
				}
				rainstormLog.Printf("Channel closed, now we can ACK!\n")
				opData.SendAck <- true
			} else {
				handleEmptyOutput(opData, input.AckInfo)
			}

		case Rainstorm_tuple_t:
			rainstormLog.Printf("Processing a single tuple!\n")
			if v.Key == FILTERED && v.Value == FILTERED {
				handleEmptyOutput(opData, input.AckInfo)
			} else {
				opData.Output <- OutputInfo{
					Tup: v,
					UID: input.AckInfo.UID + ":" + strconv.Itoa(opsDone),
					AckInfo: input.AckInfo,
				}
				// send ack after each output has been ACKed
				opData.SendAck <- true
			}
		case []Rainstorm_tuple_t:
			rainstormLog.Printf("Processing a slice of tuples!\n")
			if len(v) == 0 {
				// empty slice
				handleEmptyOutput(opData, input.AckInfo)
			} else {
				for i := 0; i < len(v); i++ {
					opData.Output <- OutputInfo{
						Tup: v[i],
						UID: input.AckInfo.UID + ":" + strconv.Itoa(opsDone),
						AckInfo: input.AckInfo,
					}
					
					if i == len(v) - 1 {
						// send ack after last tuple in slice has been ACKed
						opData.SendAck <- true
					} else {
						opData.SendAck <- false
					}
					
					opsDone++
				}
			}
		default:
		  rainstormLog.Printf("Unexpected output type: %T\n", output)
		}
	  }
	close(opData.Output) // Close the output channel when input channel is closed
} 
  
// Function to print the contents of the output channel
func processOutputChannel(opData OperatorData, port string) {
	for out := range opData.Output {

		rainstormLog.Printf("ProcessOutput: Trying to process output: %s:%s\n", out.Tup.Key, out.Tup.Value)
		for {
			nextStageArgs := GetNextStageArgs{
				Rt: out.Tup,
				Port: port,
				CurrOperator: opData.Op,
			}
			
			err := sendToNextStage(nextStageArgs, out.UID)
			if err != nil {
				rainstormLog.Printf("Error on next stage send: %v\n", err)
				time.Sleep(RAINSTORM_ACK_TIMEOUT)
			} else {
				err = handleAcks(opData, out)
				if err != nil {
					rainstormLog.Printf("Error on ACK handling: %v\n", err)
					// if our error was due to something other than timeout, wait a little bit
					if err.Error() != "TIMEOUT" && err.Error() != "OLDACK" {
						time.Sleep(RAINSTORM_ACK_TIMEOUT)
					}
				} else {
					break
				}
			}
			rainstormLog.Printf("Transmission failed or ack timed out, trying again...\n")
		}
	}
}

func handleAcks(opData OperatorData, out OutputInfo) error {
	select {
	// only want to send out acks after we've received acks
	case recvdUID := <- opData.RecvdAck:
		if recvdUID != out.UID {
			rainstormLog.Printf("Old ack %s received, expected %s dropping\n", recvdUID, out.UID)
			return fmt.Errorf("OLDACK") // we've just been rescheduled and have gotten an ack for some old data; drop it like it's hot
		} else {
			if <- opData.SendAck {
				// to write the UID that we got sent, we need to take the UID from out.AckInfo (which is from input and therefore the previous stage)
				err := writeRainstormLogs(opData.Op, opData.LogFile, opData.StateFile, out.AckInfo.UID, Rainstorm_tuple_t{out.Tup.Key, out.Tup.Value})
				if err != nil {
					return fmt.Errorf("Error writing logs: %v\n", err)
				}
				
				shiftUIDBuf(opData)

				// whether or not this goes through is not super important to us; we've already written it to output
				ackPrevStage(out.AckInfo)
			}
			return nil
		}
	case <-time.After(RAINSTORM_ACK_TIMEOUT):
		return fmt.Errorf("TIMEOUT")
	}
}

func shiftUIDBuf(opData OperatorData) {
	// once we've sent out an ack, we can remove data from our buffer (we now have another record that says we've processed the data)
	opData.UIDBufLock.Lock()
	bufLocal := *opData.UIDBuf

	for i := 0; i < len(bufLocal) - 1; i++ {
		bufLocal[i] = bufLocal[i + 1]
	}
	bufLocal[len(bufLocal) - 1] = EMPTY

	for i := 0; i < len(bufLocal); i++ {
		rainstormLog.Printf("After shift: UID at index %d: %s\n", i, bufLocal[i])
	}

	*opData.UIDBuf = bufLocal
	opData.UIDBufLock.Unlock()
}

func writeRainstormLogs(operatorName string, logFile string, stateFile string, prevUID string, stateTup Rainstorm_tuple_t) error {
	backgroundWrite(prevUID + "\n", logFile)
	if operators[operatorName].Stateful {
		backgroundWrite(fmt.Sprintf("%s:%s\n", stateTup.Key, stateTup.Value), stateFile)
	}

	return nil
}

func handleEmptyOutput(opData OperatorData, ackInfo Ack_info_t) {
	// since we have no data to send, we can't go through the standard output->send data->receive ack process
	// just send the ack from here instead
	// TODO: can we have stateful operators that also filter?
	err := writeRainstormLogs(opData.Op, opData.LogFile, opData.StateFile, ackInfo.UID, Rainstorm_tuple_t{"",""})
	if err != nil {
		rainstormLog.Printf("Error writing logs on empty ACK")
	}
	shiftUIDBuf(opData)
	err = ackPrevStage(ackInfo)
	if err != nil {
		rainstormLog.Printf("Error on empty channel ACK: %v\n", err)
	}
}
