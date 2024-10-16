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
	IP          string
	Timestamp   int64
	Incarnation int64
}

var dropProbability float64 = 0.00 // Example: 5% drop rate

// Function to determine if a message should be dropped
func shouldDropMessage() bool {
	return rand.Float64() < dropProbability
}

const introducerIP = "172.22.94.178"

var (
	membershipList         = make(map[string]Member)
	membershipListMutex    = &sync.RWMutex{}
	lastPingSentAt         = make(map[string]int64)
	lastAckReceivedAt      = make(map[string]int64)
	lastPingMutex          = &sync.Mutex{}
	lastAckReceivedMutex   = &sync.Mutex{}
	suspectedNodes         = make(map[string]bool)
	suspectedNodesMutex    = &sync.Mutex{}
	incarnationNumber      = int64(0)
	incarnationNumberMutex = &sync.Mutex{}
)

// Variables for logging
var (
	logFile   *os.File
	logger    *log.Logger
	startTime time.Time
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

var (
	UDPport        = "5000"          // The UDP port to use for this daemon
	TCPport        = "5001"          // The TCP port to use for this daemon
	pingInterval   = 1 * time.Second // Interval for sending pings
	pingTimeout    = 4 * time.Second // Time to consider a member as failed
	suspicionTimer = 5 * time.Second
)

func updateTimerValues() {
	if suspicionEnabled {
		pingInterval = 1500 * time.Millisecond
		pingTimeout = 3 * time.Second
		suspicionTimer = 4 * time.Second
	} else {
		pingInterval = 1 * time.Second
		pingTimeout = 4 * time.Second
		suspicionTimer = 5 * time.Second // This won't be used, but set for completeness
	}
}

var selfIP = GetOutboundIP().String()

func main() {
	var err error
	logFile, err = os.OpenFile("mp2.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Failed to open log file:", err)
		os.Exit(1)
	}
	defer logFile.Close()

	logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
	startTime = time.Now()

	go startUDPServer()
	go startTCPServer()
	go startPinging()
	go commandListener()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	logger.Printf("%s was actually terminated at %d", selfIP, time.Now().Unix())
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
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("Error reading from UDP: %v\n", err)
			continue
		}

		message := strings.TrimSpace(string(buf[:n]))
		// Log the received message
		logger.Printf("Received UDP message from %s: %s (size: %d bytes)", addr.String(), message, n)
		handleMessage(message)
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
			// fmt.Printf("Error accepting TCP connection: %v\n", err)
			continue
		}

		go func(c net.Conn) {
			defer c.Close()

			buf := make([]byte, 1024)
			n, err := c.Read(buf)
			if err != nil {
				// fmt.Printf("Error reading from TCP connection: %v\n", err)
				return
			}
			message := strings.TrimSpace(string(buf[:n]))
			// Log the received message
			remoteAddr := c.RemoteAddr().String()
			logger.Printf("Received TCP message from %s: %s (size: %d bytes)", remoteAddr, message, n)

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
		case "list_sus":
			listSuspectedNodes()
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

	messageBytes := []byte(message)
	bytesSent, err := conn.Write(messageBytes)
	if err != nil {
		fmt.Printf("Error writing message to %s: %v\n", targetIP, err)
	} else {
		// Log the message sent
		logger.Printf("Sent UDP message to %s: %s (size: %d bytes)", targetIP, message, bytesSent)
	}
}

func sendMessageViaTCP(targetIP, message string) {
	address := net.JoinHostPort(targetIP, TCPport)
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		// fmt.Printf("TCP connection error to %s: %v\n", targetIP, err)
		return
	}
	defer conn.Close()

	messageBytes := []byte(message)
	bytesSent, err := conn.Write(messageBytes)
	if err != nil {
		// fmt.Printf("Error sending TCP message to %s: %v\n", targetIP, err)
	} else {
		// Log the message sent
		logger.Printf("Sent TCP message to %s: %s (size: %d bytes)", targetIP, message, bytesSent)
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
	updateTimerValues()
	fmt.Println("Suspicion mechanism enabled.")
	sendToAll("SUS_ON")
}

func disableSuspicion() {
	suspicionEnabled = false
	updateTimerValues()
	fmt.Println("Suspicion mechanism disabled.")
	sendToAll("SUS_OFF")
}

func markNodeAsSuspected(ip string) {
	suspectedNodesMutex.Lock()
	suspectedNodes[ip] = true
	suspectedNodesMutex.Unlock()
	logger.Printf("Node %s marked as suspected", ip)
}

func isNodeSuspected(ip string) bool {
	suspectedNodesMutex.Lock()
	defer suspectedNodesMutex.Unlock()
	return suspectedNodes[ip]
}

func removeNodeFromSuspected(ip string) {
	suspectedNodesMutex.Lock()
	delete(suspectedNodes, ip)
	suspectedNodesMutex.Unlock()
	logger.Printf("Node %s removed from suspected list", ip)
}

func disseminateSuspicion(ip string) {
	memberIncarnation := getMemberIncarnation(ip)
	listSuspectedNodes()
	fmt.Printf("I suspect %s, telling everyone else\n", ip)
	sendToAll(fmt.Sprintf("SUSPECT,%s,%d", ip, memberIncarnation))
}

func startSuspicionTimer(ip string) {
	go func() {
		time.Sleep(suspicionTimer)
		if isNodeSuspected(ip) {
			logger.Printf("Suspicion timeout for %s, marking as failed.", ip)
			removeMember(ip)
			removeNodeFromSuspected(ip)
			sendToAll(fmt.Sprintf("LEAVE,%s", ip))
			logger.Printf("%s failure detected at %d", ip, time.Now().Unix())
		}
	}()
}

func getMemberIncarnation(ip string) int64 {
	membershipListMutex.Lock()
	defer membershipListMutex.Unlock()
	if member, exists := membershipList[ip]; exists {
		return member.Incarnation
	}
	return 0
}

func joinGroup() {
	var join_ts = fmt.Sprintf("%d", time.Now().Unix())
	if selfIP == introducerIP {
		fmt.Printf("I have joined as the introducer\n")
		addMember(introducerIP, join_ts, fmt.Sprintf("%d", incarnationNumber))
	} else {
		fmt.Printf("Trying to join the group\n")
		sendMessageViaTCP(introducerIP, fmt.Sprintf("JOIN,%s,%s,%d", selfIP, join_ts, getIncarnationNumber()))
	}
}

func listSelf() {
	fmt.Printf("I am %s \n", selfIP)
}

func listMembership() {
	fmt.Println("Current Membership List:")
	membershipListMutex.RLock()
	defer membershipListMutex.RUnlock()

	for _, member := range membershipList {
		fmt.Printf("IP: %s, I have been alive for : %d seconds, Incarnation: %d\n", member.IP, time.Now().Unix()-member.Timestamp, member.Incarnation)
	}
}

func removeMember(ip string) {
	membershipListMutex.Lock()
	delete(membershipList, ip)
	membershipListMutex.Unlock()
	logger.Printf("Node %s removed from membership list", ip)
}

func incrementIncarnation() {
	incarnationNumberMutex.Lock()
	incarnationNumber++
	incarnationNumberMutex.Unlock()
}

func getIncarnationNumber() int64 {
	incarnationNumberMutex.Lock()
	defer incarnationNumberMutex.Unlock()
	return incarnationNumber
}

func checkMembershipList(ip string) (Member, bool) {
	membershipListMutex.RLock()
	defer membershipListMutex.RUnlock()
	member, exists := membershipList[ip]
	return member, exists
}

func addMember(ip, timestamp, incarnation string) {
	convertedTS, _ := strconv.ParseInt(timestamp, 10, 64)
	convertedInc, _ := strconv.ParseInt(incarnation, 10, 64)

	membershipListMutex.Lock()
	defer membershipListMutex.Unlock()

	if ip == selfIP {
		// For self, always use the local incarnation number
		membershipList[ip] = Member{ip, convertedTS, getIncarnationNumber()}
	} else {
		// For other nodes, use the received incarnation number
		member, exists := membershipList[ip]
		if !exists || member.Incarnation < convertedInc {
			membershipList[ip] = Member{ip, convertedTS, convertedInc}
			logger.Printf("Node %s added to membership list with incarnation %d", ip, convertedInc)
		}
	}
}

func listSuspectedNodes() {
	fmt.Println("\nCurrently Suspected Nodes:")
	suspectedNodesMutex.Lock()
	defer suspectedNodesMutex.Unlock()
	for ip := range suspectedNodes {
		fmt.Println(ip)
	}
}

func updateMemberIncarnation(ip string, incarnation int64) {
	membershipListMutex.Lock()
	defer membershipListMutex.Unlock()

	if ip == selfIP {
		// For self, only update if the received incarnation is higher
		if incarnation > getIncarnationNumber() {
			incarnationNumberMutex.Lock()
			incarnationNumber = incarnation
			incarnationNumberMutex.Unlock()
		}
	} else {
		// For other nodes, update if the received incarnation is higher
		if member, exists := membershipList[ip]; exists && member.Incarnation < incarnation {
			member.Incarnation = incarnation
			membershipList[ip] = member
		}
	}
}

func sendRefutation() {
	sendToAll(fmt.Sprintf("REFUTE,%s,%d", selfIP, getIncarnationNumber()))
}

func sendFullMembershipList(targetIP string) {
	for ip, member := range membershipList {
		if ip != targetIP {
			sendMessage(targetIP, fmt.Sprintf("NEW_MEMBER,%s,%d,%d", member.IP, member.Timestamp, member.Incarnation))
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

				_, exists := membershipList[ip]
				_, suspicionExists := suspectedNodes[ip]
				if exists {
					if lastAckTime < lastPingTime {
						if suspicionEnabled {
							if !suspicionExists {
								markNodeAsSuspected(ip)
								disseminateSuspicion(ip)
								startSuspicionTimer(ip)
							}
						} else {
							logger.Printf("No ACK from %s, marking as failed.", ip)
							removeMember(ip)
							sendToAll(fmt.Sprintf("LEAVE,%s", ip))
						}
					}
				} else {
					// Node already removed
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

// Handle incoming messages
func handleMessage(msg string) {
	if shouldDropMessage() {
		logger.Printf("Simulated message drop on receive: %s", msg)
		return // Ignore this message as if it was dropped
	}

	// Log the received message (already logged in receive functions)
	parts := strings.Split(msg, ",")
	command := parts[0]

	switch command {
	case "SUS_OFF":
		suspicionEnabled = false
	case "SUS_ON":
		suspicionEnabled = true
	// Only the introducer can ingest JOIN messages
	case "JOIN":
		sender_ip := parts[1]
		ts := parts[2]
		inc := parts[3]
		_, exists := checkMembershipList(selfIP)
		if !exists {
			fmt.Print("someone tried to join the group but i am not here")
			return
		}
		addMember(sender_ip, ts, inc)
		sendToAll(fmt.Sprintf("NEW_MEMBER,%s,%s,%s", sender_ip, ts, inc))
		sendFullMembershipList(sender_ip)
		if suspicionEnabled {
			sus_status := "SUS_ON"
			sendMessageViaTCP(sender_ip, sus_status)
		} else {
			sus_status := "SUS_OFF"
			sendMessageViaTCP(sender_ip, sus_status)
		}
	case "NEW_MEMBER":
		sender_ip := parts[1]
		ts := parts[2]
		inc := parts[3]
		addMember(sender_ip, ts, inc)
	case "LEAVE":
		leaver_ip := parts[1]
		logger.Printf("Received LEAVE message from %s", leaver_ip)
		if leaver_ip == selfIP {
			fmt.Printf("I was killed unfortunately, if only you had enabled suspicion earlier\n\n\n :(")
			os.Exit(0)
		}
		removeMember(leaver_ip)
	case "PING":
		senderIP := parts[1]
		// Send ACK back to the sender
		sendMessage(senderIP, fmt.Sprintf("ACK,%s", selfIP))
	case "ACK":
		senderIP := parts[1]
		timestamp := time.Now().Unix()
		updateLastAckReceived(senderIP, timestamp)
		removeNodeFromSuspected(senderIP)
	case "SUSPECT":
		if !suspicionEnabled {
			return
		}
		suspectedIP := parts[1]
		suspectedIncarnation, _ := strconv.ParseInt(parts[2], 10, 64)
		if suspectedIP == selfIP {
			if suspectedIncarnation >= getIncarnationNumber() {
				incrementIncarnation()
				sendRefutation()
			}
		} else {
			updateMemberIncarnation(suspectedIP, suspectedIncarnation)
			markNodeAsSuspected(suspectedIP)
			startSuspicionTimer(suspectedIP)
		}
		listSuspectedNodes()

	case "REFUTE":
		if !suspicionEnabled {
			return
		}
		refutingIP := parts[1]
		refuteIncarnation, _ := strconv.ParseInt(parts[2], 10, 64)
		updateMemberIncarnation(refutingIP, refuteIncarnation)
		removeNodeFromSuspected(refutingIP)
	}
	// This is where bulk of the SWIM logic will go
}
