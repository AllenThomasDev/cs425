package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"
)

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
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			buf := make([]byte, 1024)
			n, err := c.Read(buf)
			if err != nil {
				return
			}
			message := strings.TrimSpace(string(buf[:n]))
			remoteAddr := c.RemoteAddr().String()
			logger.Printf("Received TCP message from %s: %s (size: %d bytes)", remoteAddr, message, n)
			handleMessage(message)
		}(conn)
	}
}

func sendMessageViaTCP(targetIP, message string) {
	address := net.JoinHostPort(targetIP, TCPport)
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	messageBytes := []byte(message)
	bytesSent, err := conn.Write(messageBytes)
	if err != nil {
	} else {
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
	sendMessageViaTCP(targetIP, fmt.Sprintf("PING,%s,%d", selfIP, ping_ts))
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
