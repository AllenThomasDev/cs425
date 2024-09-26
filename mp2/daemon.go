package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Member struct {
	IP        string
	Port      string
	Timestamp int64
}

var vmIPs = map[string]string{
	"vm1": "172.22.94.178",
	"vm2": "172.22.156.179",
	// "vm3":  "172.22.158.179",
	// "vm4":  "172.22.94.179",
	// "vm5":  "172.22.156.180",
	// "vm6":  "172.22.158.180",
	// "vm7":  "172.22.94.180",
	// "vm8":  "172.22.156.181",
	// "vm9":  "172.22.158.181",
	// "vm10": "172.22.94.181",
}
var membershipList = make(map[string]Member)

const (
	port         = "5000"          // The UDP port to use for this daemon
	pingInterval = 2 * time.Second // Interval for sending pings
	timeout      = 5 * time.Second // Time to consider a member as failed
)

func main() {
	go startUDPServer()

	// Start the pinging process
	go startPinging()

	// Graceful shutdown handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	fmt.Println("Shutting down daemon...")
}

// Start the UDP server to listen for incoming messages from other daemons
func startUDPServer() {
	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		fmt.Printf("Error resolving address: %v\n", err)
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("Error starting UDP server: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Printf("Daemon is listening on %s\n", port)

	buf := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("Error reading from UDP: %v\n", err)
			continue
		}

		handleMessage(strings.TrimSpace(string(buf[:n])))
	}
}

// Handle incoming UDP messages
func handleMessage(msg string) {
	parts := strings.Split(msg, ",")
	if len(parts) != 3 {
		fmt.Printf("Invalid message received: %s\n", msg)
		return
	}

	ip, port := parts[0], parts[1]
	timestamp, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		fmt.Printf("Invalid timestamp: %v\n", err)
		return
	}

	membershipList[ip] = Member{IP: ip, Port: port, Timestamp: timestamp}
	fmt.Printf("Received heartbeat from %s:%s\n", ip, port)
}

// Start sending ping messages to other daemons
func startPinging() {
	for {
		for _, server := range vmIPs {
			go sendPing(server)
		}
		time.Sleep(pingInterval)
	}
}

// Send a ping message to another daemon
func sendPing(serverAddress string) {
	conn, err := net.Dial("udp", serverAddress+":"+port)
	if err != nil {
		fmt.Printf("Error dialing UDP: %v\n", err)
		return
	}
	defer conn.Close()

	localIP := "127.0.0.1" // Change this to your actual IP address
	msg := fmt.Sprintf("%s,%s,%d", localIP, port, time.Now().Unix())

	_, err = conn.Write([]byte(msg))
	if err != nil {
		fmt.Printf("Error sending ping to %s: %v\n", serverAddress, err)
	} else {
		fmt.Printf("Sent ping to %s\n", serverAddress)
	}
}
