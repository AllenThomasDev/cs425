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
		addMember(sender_ip, ts, inc, NEW_MEMBER)
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
		addMember(sender_ip, ts, inc, NEW_MEMBER)
	case "OLD_MEMBER":
		sender_ip := parts[1]
		ts := parts[2]
		inc := parts[3]
		addMember(sender_ip, ts, inc, OLD_MEMBER)
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
	case "MERGE":
		hyDFSFilename := parts[1]
		senderIP := parts[2]
		fmt.Printf("Merging file %s for host %d\n", hyDFSFilename, ipToVM(senderIP))
		forwardMerge(hyDFSFilename)
		err := mergeFile(hyDFSFilename, fileLogs[hyDFSFilename])
		if err != nil {
			fmt.Printf("Error merging file: %v\n", err)
			sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", ERROR_ACK))
			return
		}
		sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", GOOD_ACK))
	
	// Note: we don't use RPCs for these two cases since we don't care if they complete or not.
	// if we get a connection error on a merge, the merged file will be replicated at the next node post-merge,
	// and if we get a connection error on a removal, well the files are gone from the system
	case "MERGE_FORWARD":
		hyDFSFilename := parts[1]
		encodedLog := parts[2]
		fmt.Printf("Forwarded merge for file %s\n", hyDFSFilename)
		decodedLog := decodeFileLog(encodedLog)
		err := mergeFile(hyDFSFilename, decodedLog)
		if err != nil {
			fmt.Printf("Error merging %s: %v\n", hyDFSFilename, err)
		}
	case "REMOVE":
		err := removeFiles(parts[1:])
		if err != nil {
			fmt.Printf("Error removing files: %v\n", err)
		}
	}
}
