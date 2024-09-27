package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var suspicionEnabled = false

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

var vmList []string

var membershipList = make(map[string]Member)

const (
	port         = "5000"          // The UDP port to use for this daemon
	pingInterval = 2 * time.Second // Interval for sending pings
	timeout      = 5 * time.Second // Time to consider a member as failed
)

func main() {
	vmList = make([]string, 0, len(vmIPs))

	for _, value := range vmIPs {
		vmList = append(vmList, value)
	}
	go startUDPServer()
	//	go startPinging()
	time.Sleep(2)
	go commandListener()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	fmt.Println("Shutting down daemon...")
}

// Start the UDP server to listen for incoming messages from other daemons
func startUDPServer() {
	addr, err := net.ResolveUDPAddr("udp", ":"+port)
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
	fmt.Printf(
		"my own ip is %s",
		GetOutboundIP(),
	)
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

func commandListener() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)

		switch input {
		case "list_mem":
			listMembership()
		case "list_self":
			listSelf()
		case "join":
			joinGroup()
		case "leave":
			leaveGroup()
		case "enable_sus":
			enableSuspicion()
		case "disable_sus":
			disableSuspicion()
		case "status_sus":
			statusSuspicion()
		default:
			fmt.Println("Unknown command")
		}
	}
}

func statusSuspicion() {
	fmt.Printf("Suspicion enabled == %t", suspicionEnabled)
}

func leaveGroup() {
	selfIP := GetOutboundIP().String()

	// Inform all members that this node is leaving
	for ip := range membershipList {
		if ip != selfIP {
			sendMessage(ip, fmt.Sprintf("LEAVE,%s,%s", selfIP, port))
		}
	}

	// Clear the membership list
	membershipList = make(map[string]Member)
	fmt.Println("Left the group.")
}

func sendMessage(targetIP, message string) {
	conn, err := net.Dial("udp", targetIP+":"+port)
	if err != nil {
		fmt.Printf("Error sending message to %s: %v\n", targetIP, err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("Error writing message to %s: %v\n", targetIP, err)
	}
}

func enableSuspicion() {
	suspicionEnabled = true
	fmt.Println("suspicion enabled.")
}

// use sendMessage to tell everyone what the status of suspicion is
func disableSuspicion() {
	suspicionEnabled = false
	fmt.Println("Suspicion mechanism disabled.")
}

func joinGroup() {
	fmt.Println("joinGroup")
}

func listSelf() {
	fmt.Printf("I am %s", GetOutboundIP().String())
}

func listMembership() {
	fmt.Println("Current Membership List:")
	for _, member := range membershipList {
		fmt.Printf("IP: %s, Port: %s, Timestamp: %d\n", member.IP, member.Port, member.Timestamp)
	}
}

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
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

	// This is where bulk of the SWIM logic will go
}

// Start sending ping messages to other daemons
// Send a ping message to another daemon
