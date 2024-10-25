package main

import (
	"errors"
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
	case "HYDFSACK":
		ack, err := strconv.Atoi(parts[1])
		if err != nil {
			ackChannel <- ERROR_ACK
		} else {
			ackChannel <- ack_type_t(ack)
		}
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
		senderIP := parts[3]
		timestamp := parts[4]
		vmNo, err := strconv.Atoi(parts[5])
		if err != nil {
			fmt.Printf("Error: could not extract vm number\n")
			sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", ERROR_ACK))
			return
		}
		
		aID := append_id_t{vmNo, timestamp}
		randFilename, err := appendFile(hyDFSFileName, fileContent)
		if err != nil {
			fmt.Printf("Error appending file\n")
			sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", ERROR_ACK))
			return
		}

		fmt.Printf("Append to %s at file %s from vm %d\n", hyDFSFileName, randFilename, vmNo)
		fileChannels[hyDFSFileName] <- aID
		aIDtoFile[hyDFSFileName][aID] = randFilename
		sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", GOOD_ACK))
	case "GET":
		hyDFSFileName := parts[1] // The file to be fetched from HyDFS
		localFileName := parts[2] // The local file where content will be saved
		senderIP := parts[3]      // IP of the sender requesting the file
		fmt.Printf("Received GET message from %s to fetch %s", senderIP, hyDFSFileName)
		fileContent, err := readFileToMessageBuffer(hyDFSFileName, "server")
		if err != nil {
			fmt.Printf("Error writing file content: %v\n", err)
			sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", ERROR_ACK))
			return
		}

		// get shards
		for i := 0; i < len(fileLogs[hyDFSFileName]); i++ {
			// REMINDER: aIDtoFile uses filename, append id to give us randomized filename
			shardContent, err := readFileToMessageBuffer(aIDtoFile[hyDFSFileName][fileLogs[hyDFSFileName][i]], "server")
			if err != nil {
				fmt.Printf("Error writing file content: %v\n", err)
				sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", ERROR_ACK))
				return
			}

			fileContent = fileContent + shardContent
		}
		sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", GOOD_ACK))
		response := fmt.Sprintf("FILE_CONTENT,%s,%s,%s", hyDFSFileName, fileContent, localFileName)
		sendMessageViaTCP(senderIP, response)

		fmt.Printf("\nSent file content to %s", senderIP)
	case "FILE_CONTENT":
		hyDFSFileName := parts[1] // Extract the HyDFS file name
		fileContent := parts[2]   // Extract the file content
		localFileName := parts[3] // Extract the local file name to save content
		fmt.Printf("Received FILE_CONTENT for %s\n saving to %s\n", hyDFSFileName, localFileName)
		err := writeFile(localFileName, fileContent, "client")
		if err != nil {
			fmt.Printf("Error on file receipt: %v\n", err);
		} else {
			fmt.Printf("File content saved successfully to %s\n", localFileName)
		}
	case "CREATE":
		hyDFSFileName := parts[1]
		fileContent := parts[2]
		senderIP := parts[3]
		err := writeFile(hyDFSFileName, fileContent, "server")
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				// file already exists, acknowledge to indicate we have it
				sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", GOOD_ACK))
				return
			} else {
				fmt.Printf("Error on file creation: %v\n", err);
				sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", ERROR_ACK))
				return
			}
		}

		fmt.Printf("Received CREATE message for %s and %s\n", hyDFSFileName, fileContent)
		fileChannels[hyDFSFileName] = make(chan append_id_t, 100)
		fileLogs[hyDFSFileName] = make([]append_id_t, 0)
		aIDtoFile[hyDFSFileName] = make(map[append_id_t]string, 0)
		// launch thread to manage appends
		go writeToLog(hyDFSFileName)
		sendMessageViaTCP(senderIP, fmt.Sprintf("HYDFSACK,%d", GOOD_ACK))
	case "REMOVE":
		removeFiles(parts[1:])
	}
}
