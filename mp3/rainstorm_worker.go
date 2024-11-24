package main

import (
	"fmt"
	"time"
)

var (
	stopChannels =  make(map[string] chan string)
)

func deferredStop(port string) {
	fmt.Println("About to die...")
	time.Sleep(time.Millisecond)
	// need to send TWO stop requests, one for RPC listener and one for function wrapper
	stopChannels[port] <- "die."
	stopChannels[port] <- "die."
}