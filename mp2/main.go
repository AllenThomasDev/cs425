package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	initIPList()
	go daemonMain()
	go commandListener()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	logger.Printf("%s was actually terminated at %d", selfIP, time.Now().Unix())
	fmt.Println("Shutting down daemon...")
}
