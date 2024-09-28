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
	Timestamp int64
}

const introducerIP = "172.22.94.178"

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
	if GetOutboundIP().String() == introducerIP {
		fmt.Println("I am the introducer")
	}
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
	// Inform all members that this node is leaving
	sendToAll("LEAVE")
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

func sendToAll(message string) {
	selfIP := GetOutboundIP().String()
	for ip := range membershipList {
		if ip != selfIP {
			sendMessage(ip, fmt.Sprintf("%s,%s", message, selfIP))
		}
	}
}

func enableSuspicion() {
	suspicionEnabled = true
	fmt.Println("suspicion enabled.")
	sendToAll("SUS_ON")
}

// use sendMessage to tell everyone what the status of suspicion is
func disableSuspicion() {
	suspicionEnabled = false
	fmt.Println("Suspicion mechanism disabled.")
	sendToAll("SUS_OFF")
}

func joinGroup() {
	var self_ip = GetOutboundIP().String()
	var join_ts = fmt.Sprintf("%d", time.Now().Unix())
	if self_ip == introducerIP {
		fmt.Printf("I have joined as the introducer\n")
		addMember(introducerIP, join_ts)
		return
	}
	fmt.Printf("Trying to join the group\n")
	sendMessage(introducerIP, fmt.Sprintf("JOIN,%s,%s", self_ip, join_ts))
}

func listSelf() {
	fmt.Printf("I am %s \n", GetOutboundIP().String())
}

func listMembership() {
	fmt.Println("Current Membership List:")
	for _, member := range membershipList {
		fmt.Printf("IP: %s, Timestamp: %d\n", member.IP, member.Timestamp)
	}
}
func removeMember(ip string) {
	delete(membershipList, ip)
}

func addMember(ip string, timestamp string) {
	converted_ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		panic(err)
	}
	membershipList[ip] = Member{ip, converted_ts}
}

// Sends the entire current membership list to the specified node
func sendFullMembershipList(targetIP string) {
	for ip, member := range membershipList {
		if ip != targetIP { // Don't send the new member's entry back to itself
			sendMessage(targetIP, fmt.Sprintf("NEW_MEMBER,%s,%d", member.IP, member.Timestamp))
		}
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
	switch command := parts[0]; command {
	case "SUS_OFF":
		suspicionEnabled = false
	case "SUS_ON":
		suspicionEnabled = true
		// Only the introducer can ingest JOIN messages
	case "JOIN":
		sender_ip := parts[1]
		ts := parts[2]
		addMember(sender_ip, ts) // add sender to membershipList on introc and then send this new memberslist to everyone
		sendToAll(fmt.Sprintf("NEW_MEMBER,%s,%s", sender_ip, ts))
		sendFullMembershipList(sender_ip)
	case "NEW_MEMBER":
		sender_ip := parts[1]
		ts := parts[2]
		addMember(sender_ip, ts)
	case "LEAVE":
		sender_ip := parts[1]
		removeMember(sender_ip)
		// add sender to membershipList on introc and then send this new memberslist to everyone
	}
	// This is where bulk of the SWIM logic will go
}

// Start sending ping messages to other daemons
// Send a ping message to another daemon
