package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
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

// what happens if we have to create a big file? subsequent appends can only happen AFTER the create

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
	TCPport        = "5001"          // The TCP port to use for this daemon
	pingInterval   = 1 * time.Second // Interval for sending pings
	pingTimeout    = 2 * time.Second // Time to consider a member as failed
	suspicionTimer = 5 * time.Second
)

func daemonMain() {
	var err error
	logFile, err = os.OpenFile("mp2.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Failed to open log file:", err)
		os.Exit(1)
	}
	defer logFile.Close()
	logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
	startTime = time.Now()
	go startTCPServer()
	go startPinging()
	joinGroup()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	logger.Printf("%s was actually terminated at %d", selfIP, time.Now().Unix())
	fmt.Println("Shutting down daemon...")
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

func enableSuspicion() {
	suspicionEnabled = true
	fmt.Println("Suspicion mechanism enabled.")
	sendToAll("SUS_ON")
}

func disableSuspicion() {
	suspicionEnabled = false
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
		addMember(introducerIP, join_ts, fmt.Sprintf("%d", incarnationNumber), NEW_MEMBER)
	} else {
		fmt.Printf("Trying to join the group\n")
		err := sendMessageViaTCP(introducerIP, fmt.Sprintf("JOIN,%s,%s,%d", selfIP, join_ts, getIncarnationNumber()))
		if err != nil {
			fmt.Printf("Error joining group: %v\n", err)
			os.Exit(1)
		}
	}
}

func listSelf() {
	VMID := ipToVM(selfIP)
	var hashList []int
	fmt.Printf(
		"I am %s , ID= %d",
		selfIP,
		VMID,
	)
  fmt.Printf("New files with hashes - ")
	for k, v := range routingTable {
		if v == VMID {
      hashList = append(hashList, k)
		}
	}
  fmt.Printf("%v\n", hashList)
  fmt.Printf("will be routed to this node")
  }


func listMembership() {
	memberListSorted := make([]string, 10)
	fmt.Println("Current Membership List:")
	membershipListMutex.RLock()
	defer membershipListMutex.RUnlock()
	for _, member := range membershipList {
    memberListSorted[ipToVM(member.IP)] = fmt.Sprintf("IP: %s, I have VMid= %d\n" , member.IP, ipToVM(member.IP))
	}

	for i := 0; i < len(membershipList); i++ {
		fmt.Printf(memberListSorted[i])
	}
}

func removeMember(ip string) {
	membershipListMutex.Lock()
	delete(membershipList, ip)
	membershipListMutex.Unlock()
	removeFromHyDFS(ip)
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

func addMember(ip, timestamp, incarnation string, memType member_type_t) {
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
			addToHyDFS(ip, memType)
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
			sendMessageViaTCP(targetIP, fmt.Sprintf("OLD_MEMBER,%s,%d,%d", member.IP, member.Timestamp, member.Incarnation))
		}
	}
}
