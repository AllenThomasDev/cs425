package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var suspicionEnabled = false

type Member struct {
	IP        string
	Timestamp int64
}

const k = 3 // Number of entries

const introducerIP = "172.22.94.178"

var (
	membershipList       = make(map[string]Member)
	membershipListMutex  = &sync.RWMutex{}
	lastPingSentAt       = make(map[string]int64)
	lastAckReceivedAt    = make(map[string]int64)
	lastPingMutex        = &sync.Mutex{}
	lastAckReceivedMutex = &sync.Mutex{}
)

func updateLastPingSent(ip string, timestamp int64) {
	lastPingMutex.Lock()
	defer lastPingMutex.Unlock()
	lastPingSentAt[ip] = timestamp
}

func getLastPingSent(ip string) int64 {
	lastPingMutex.Lock()
	defer lastPingMutex.Unlock()
	return lastPingSentAt[ip]
}

func updateLastAckReceived(ip string, timestamp int64) {
	lastAckReceivedMutex.Lock()
	defer lastAckReceivedMutex.Unlock()
	lastAckReceivedAt[ip] = timestamp
}

func getLastAckReceived(ip string) int64 {
	lastAckReceivedMutex.Lock()
	defer lastAckReceivedMutex.Unlock()
	return lastAckReceivedAt[ip]
}

const (
	UDPport      = "5000"          // The UDP port to use for this daemon
	TCPport      = "5001"          // The UDP port to use for this daemon
	pingInterval = 1 * time.Second // Interval for sending pings
	pingTimeout  = 4 * time.Second // Time to consider a member as failed
)

var selfIP = GetOutboundIP().String()

func main() {
	go startUDPServer()
	go startTCPServer()
	go startPinging()
	go commandListener()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	fmt.Println("Shutting down daemon...")
}

// Start the UDP server to listen for incoming messages from other daemons
func startUDPServer() {
	addr, err := net.ResolveUDPAddr("udp", ":"+UDPport)
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

	fmt.Printf("Daemon is listening on %s\n", UDPport)

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

func startTCPServer() {
	listener, err := net.Listen("tcp", ":"+TCPport)
	if err != nil {
		fmt.Printf("Error starting TCP server: %v\n", err)
		return
	}
	defer listener.Close()

	fmt.Printf("TCP server is listening on %s\n", TCPport)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting TCP connection: %v\n", err)
			continue
		}

		go func(c net.Conn) {
			defer c.Close()

			buf := make([]byte, 1024)
			n, err := c.Read(buf)
			if err != nil {
				fmt.Printf("Error reading from TCP connection: %v\n", err)
				return
			}
			message := strings.TrimSpace(string(buf[:n]))
			handleMessage(message)
		}(conn)
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
	conn, err := net.Dial("udp", targetIP+":"+UDPport)
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

func sendMessageViaTCP(targetIP, message string) {
	address := net.JoinHostPort(targetIP, "5001")
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		fmt.Printf("TCP connection error to %s: %v\n", targetIP, err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("Error sending TCP message to %s: %v\n", targetIP, err)
	}
}

func sendToAll(message string) {
	for ip := range membershipList {
		if ip != selfIP {
			sendMessageViaTCP(ip, fmt.Sprintf("%s,%s", message, selfIP))
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
	var join_ts = fmt.Sprintf("%d", time.Now().Unix())
	if selfIP == introducerIP {
		fmt.Printf("I have joined as the introducer\n")
		addMember(introducerIP, join_ts)
		return
	}
	fmt.Printf("Trying to join the group\n")
	sendMessageViaTCP(introducerIP, fmt.Sprintf("JOIN,%s,%s", selfIP, join_ts))
}

func listSelf() {
	fmt.Printf("I am %s \n", selfIP)
}

func listMembership() {
	fmt.Println("Current Membership List:")
	for _, member := range membershipList {
		fmt.Printf("IP: %s, Timestamp: %d\n", member.IP, member.Timestamp)
	}
}

func removeMember(ip string) {
	membershipListMutex.Lock()
	delete(membershipList, ip)
	membershipListMutex.Unlock()
}

func addMember(ip string, timestamp string) {
	converted_ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		panic(err)
	}
	membershipListMutex.Lock()
	membershipList[ip] = Member{ip, converted_ts}
	membershipListMutex.Unlock()
}

// Sends the entire current membership list to the specified node
func sendFullMembershipList(targetIP string) {
	for ip, member := range membershipList {
		if ip != targetIP { // Don't send the new member's entry back to itself
			sendMessage(targetIP, fmt.Sprintf("NEW_MEMBER,%s,%d", member.IP, member.Timestamp))
		}
	}
}

func startPinging() {
	for {
		targetIP := selectRandomMember()
		if targetIP != "" {
			ping(targetIP)
			// Start a goroutine to handle the timeout asynchronously
			go func(ip string) {
				time.Sleep(pingTimeout)

				// Capture the necessary timestamps without holding the locks
				lastPingMutex.Lock()
				lastPingTime := lastPingSentAt[ip]
				lastPingMutex.Unlock()

				lastAckReceivedMutex.Lock()
				lastAckTime := lastAckReceivedAt[ip]
				lastAckReceivedMutex.Unlock()

				if lastAckTime < lastPingTime {
					fmt.Printf("No ACK received from %s, marking as failed.\n", ip)
					removeMember(ip)
					sendToAll(fmt.Sprintf("LEAVE,%s", ip))
				}
			}(targetIP)
		}
		time.Sleep(pingInterval)
	}
}

func ping(targetIP string) {
	var ping_ts = time.Now().Unix()
	sendMessage(targetIP, fmt.Sprintf("PING,%s,%d", selfIP, ping_ts))
	updateLastPingSent(targetIP, ping_ts)
}

func selectRandomMember() string {
	var eligibleMembers []string
	for ip := range membershipList {
		if ip != selfIP { // Exclude selfIP
			eligibleMembers = append(eligibleMembers, ip)
		}
	}
	if len(eligibleMembers) == 0 {
		return ""
	}
	randomIndex := rand.Intn(len(eligibleMembers))
	return eligibleMembers[randomIndex]
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

		// Check if the introducer itself is in the membership list
		if _, exists := membershipList[introducerIP]; !exists {
			fmt.Printf("Introducer (%s) not in membership list\n", introducerIP)
			break
		}
		// Now add the joining member
		addMember(sender_ip, ts)                                  // Add sender to the membership list
		sendToAll(fmt.Sprintf("NEW_MEMBER,%s,%s", sender_ip, ts)) // Inform others about the new member
		sendFullMembershipList(sender_ip)                         // Send the full membership list to the new member
	case "NEW_MEMBER":
		sender_ip := parts[1]
		ts := parts[2]
		addMember(sender_ip, ts)
	case "LEAVE":
		sender_ip := parts[1]
		removeMember(sender_ip)
	case "PING":
		senderIP := parts[1]
		// Send ACK back to the sender
		sendMessage(senderIP, fmt.Sprintf("ACK,%s", selfIP))
	case "ACK":
		senderIP := parts[1]
		timestamp := time.Now().Unix()
		updateLastAckReceived(senderIP, timestamp)
	}
	// This is where bulk of the SWIM logic will go
}

// Start sending ping messages to other daemons
// Send a ping message to another daemon
