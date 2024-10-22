package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func handleMessage(msg string) {
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
		sendMessageViaTCP(senderIP, fmt.Sprintf("ACK,%s", selfIP))
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
	case "APPEND":
		fmt.Println("Received APPEND message")
		hyDFSFileName := parts[1]
		fileContent := parts[2]
		appendFile(hyDFSFileName, fileContent)
	case "GET":
		hyDFSFileName := parts[1] // The file to be fetched from HyDFS
		localFileName := parts[2] // The local file where content will be saved
		senderIP := parts[3]      // IP of the sender requesting the file
		fmt.Printf("Received GET message from %s to fetch %s", senderIP, hyDFSFileName)
		fileContent := readFileToMessageBuffer(hyDFSFileName)
		response := fmt.Sprintf("FILE_CONTENT,%s,%s,%s", hyDFSFileName, fileContent, localFileName)
		sendMessageViaTCP(senderIP, response)

		fmt.Printf("\nSent file content to %s", senderIP)

	case "FILE_CONTENT":
		hyDFSFileName := parts[1] // Extract the HyDFS file name
		fileContent := parts[2]   // Extract the file content
		localFileName := parts[3] // Extract the local file name to save content
		fmt.Printf("Received FILE_CONTENT for %s\n saving to %s", hyDFSFileName, localFileName)
		writeFile(localFileName, fileContent)
		fmt.Printf("File content saved successfully to %s", localFileName)
	case "CREATE":
		hyDFSFileName := parts[1]
		fileContent := parts[2]
		writeFile(hyDFSFileName, fileContent)
		fmt.Printf("Received CREATE message for %s and %s", hyDFSFileName, fileContent)
	}
}
