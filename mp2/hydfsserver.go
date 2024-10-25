package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
)
var (
	routingTableMutex = &sync.RWMutex{}
	successorsMutex = &sync.RWMutex{}
	fileChannels = make(map[string]chan append_id_t) // channel used to write to fileLogs
	fileLogs = make(map[string] []append_id_t) // log of appends to file in order received
	aIDtoFile = make(map[string]map[append_id_t]string) // map linking append ids to random filenames
	ackChannel = make(chan ack_type_t, 3) // channel allowing us to enable timeouts on acks
)

func writeToLog(fileName string) {
	for {
		appendID, open := <-(fileChannels[fileName])
		// if channel has been closed, file has been sent somewhere else
		if !open {
			delete(fileChannels, fileName)
			delete(fileLogs, fileName)
			delete(aIDtoFile, fileName)
		} else {
			fileLogs[fileName] = append(fileLogs[fileName], appendID)
		}	
	}
}

func genRandomFileName() string {
	return(strconv.Itoa(int(rand.Int31())))
}

func addToHyDFS(ip string, memType member_type_t) {
	addToRoutingTable(ipToVM(ip))
	insertIndex := addToSuccessors(ipToVM(ip))
	// if we aren't the one being added to the network, and we have an insert at index 0 or 1, data owned by this node must be replicated
	if memType == NEW_MEMBER && insertIndex > -1 && insertIndex < 2 {
		routingTableMutex.RLock()
		ownedFiles := findOwnedFiles()
		routingTableMutex.RUnlock()
		
		// if we have owned files, replicate them
		if len(ownedFiles) > 0 {
			ack := replicateFiles(ip, ownedFiles)
			if ack == TIMEOUT_ACK || ack == ERROR_ACK {
				fmt.Printf("Error: replication timed out or otherwise failed\n")
				if PANIC_ON_ERROR == 1 {
					panic("System in undetermined state")
				}
				return
			}
			// if we have > 3 successors (last one is current node), the node at index 2 has replicas it shouldn't
			successorsMutex.RLock()
			defer successorsMutex.RUnlock()
			if len(successors) > 3 {
				sendMessageViaTCP(vmToIP(successors[2]), fmt.Sprintf("REMOVE,%s", strings.Join(ownedFiles, ",")))
			}
		}
	}
}

func removeFromHyDFS(ip string) {
	removeFromRoutingTable(ipToVM(ip))
	removeIndex := removeFromSuccessors(ipToVM(ip))

	// If we have more than one successor (meaning successors will contain 2 nodes including us)
	// and one of our previous successors was removed, OR if the node before us was removed and we
	// now own new files, forward to the new successor at index 1

	successorsMutex.RLock()
	defer successorsMutex.RUnlock()
	routingTableMutex.RLock()
	defer routingTableMutex.RUnlock()

	if len(successors) > 2 && ((removeIndex > -1 && removeIndex < 2) || routingTable[ipToVM(ip)] == currentVM) {
		ownedFiles := findOwnedFiles()
		if len(ownedFiles) > 0 {
			ack := replicateFiles(vmToIP(successors[1]), ownedFiles)
			// only need to panic on ERROR_ACK since on timeout failure detector will eventually call 
			// removeFromHyDFS on timed out node which will call replicate again
			if ack == ERROR_ACK {
				fmt.Printf("Error on replication, system in undetermined state\n")
				if PANIC_ON_ERROR == 1 {
					panic("System in undetermined state")
				}
			}
		}
	}
}

func replicateFiles(ip string, repFiles []string) ack_type_t {
	for i := 0; i < len(repFiles); i++ {
		// write initial file (data written from CREATE call)
		fmt.Printf("Replicating file %s at vm %d\n", repFiles[i], ipToVM(ip))
		fileContent, err := readFileToMessageBuffer(repFiles[i], "server")
		if err != nil {
			fmt.Printf("Error reading file content: %v\n", err)
			return ERROR_ACK
		}

		err = sendMessageViaTCP(ip, fmt.Sprintf("CREATE,%s,%s,%s", repFiles[i], fileContent, selfIP))
		if err != nil {
			fmt.Printf("TCP error: %v\n", err)
			return ERROR_ACK
		}

		ack := waitForAck()
		if ack == TIMEOUT_ACK || ack == ERROR_ACK {
			return ack
		}

		// write shards (data from APPEND calls)
		for j := 0; j < len(fileLogs[repFiles[i]]); j++ {
			// REMINDER: aIDtoFile uses filename, append id to give us randomized filename
			shardContent, err := readFileToMessageBuffer(aIDtoFile[repFiles[i]][fileLogs[repFiles[i]][j]], "server")
			if err != nil {
				fmt.Printf("Error reading shard content: %v\n", err)
				return ERROR_ACK
			}

			err = sendMessageViaTCP(ip, fmt.Sprintf("APPEND,%s,%s,%s,%s,%d", repFiles[i], shardContent, selfIP,
									fileLogs[repFiles[i]][j].timestamp, fileLogs[repFiles[i]][j].vm))
			if err != nil {
				fmt.Printf("TCP error: %v\n", err)
				return ERROR_ACK
			}
			
			if ack == TIMEOUT_ACK || ack == ERROR_ACK {
				return ack
			}
		}
	}
	return GOOD_ACK
}

func findOwnedFiles() []string {
	// DO NOT LOCK: called from functions which already lock
	fileList := make([]string, 0)
	for k, _ := range(fileLogs) {
		if routingTable[hash(k)] == currentVM {
			fileList = append(fileList, k)
			fmt.Printf("File %s owned\n", k)
		}
	}
	return fileList
}

func addToSuccessors(hash int) int {
	successorsMutex.Lock()
	defer successorsMutex.Unlock()
	
	found, insertIndex := searchSuccessors(hash)
	if !found {
		if insertIndex == len(successors) - 1 {
			temp := successors[len(successors) - 1]
			successors[len(successors) - 1] = hash
			successors = append(successors, temp)
		} else {
			lastElement := currentVM
			copy(successors[insertIndex+1:], successors[insertIndex:])
			successors = append(successors, lastElement)
			successors[insertIndex] = hash
		}
		
		//printSuccessors()
		return insertIndex
	}
	//printSuccessors()
	return -1
}

func removeFromSuccessors(hash int) int {
	successorsMutex.Lock()
	defer successorsMutex.Unlock()

	found, index := searchSuccessors(hash)
	if found {
		successors = append(successors[0:index], successors[index+1:]...)
		return index
	}

	//printSuccessors()
	return -1
}

func addToRoutingTable(hash int) {
	routingTableMutex.Lock()
	defer routingTableMutex.Unlock()

	// routing table is current if new hash maps to respective VM
	if (routingTable[hash] != hash) {
		routingTable[hash] = hash

		// change entries in routing table between new node and previous node
		nextLowest := mod((hash - 1), MACHINES_IN_NETWORK)

		for {
			if nextLowest == hash || routingTable[nextLowest] == nextLowest {
				break
			}
			routingTable[nextLowest] = hash

			nextLowest = mod((nextLowest - 1), MACHINES_IN_NETWORK)
		}
	}
	//printRoutingTable()
}

func removeFromRoutingTable(hash int) {
	routingTableMutex.Lock()
	defer routingTableMutex.Unlock()

	if routingTable[hash] == hash {

		nextIP := routingTable[(hash+1)%MACHINES_IN_NETWORK]
		routingTable[hash] = nextIP

		nextLowest := mod((hash - 1), MACHINES_IN_NETWORK)

		for {
			if routingTable[nextLowest] == nextLowest || nextLowest == hash {
				break
			}
			routingTable[nextLowest] = nextIP
			nextLowest = mod((nextLowest - 1), MACHINES_IN_NETWORK)
		}
	}

	//printRoutingTable()
}

// find whether element is in list or not, and if not, index to insert
func searchSuccessors(hash int) (bool, int) {
	// DON'T LOCK: this is called from add/remove successors which already lock

	if currentVM == hash {
		return true, len(successors) - 1
	}

	// iterate through nodes that aren't current VM
	var i int
	for i = 0; i < len(successors); i++ {
		if successors[i] == hash {
			return true, i
		} else if clockwiseDistance(currentVM, successors[i]) > clockwiseDistance(currentVM, hash) {
			return false, i
		}
	}

	// insert at end
	return false, i - 1
}

func clockwiseDistance(start int, cw int) int {
	if cw < start {
		return (cw + MACHINES_IN_NETWORK) - start
	} else {
		return cw - start
	}
}

func printRoutingTable() {
	routingTableMutex.RLock()
	defer routingTableMutex.RUnlock()

	for k, v := range routingTable {
		fmt.Printf("Hash %d maps to VM %d\n", k, v)
	}
}

func printSuccessors() {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()
	for i := range successors {
		fmt.Printf("%d ", successors[i])
	}
	fmt.Printf("\n")
}

// go mod operator allows negative values
func mod(a int, b int) int {
	return (a%b + b) % b
}

func ipToVM(ip string) int {
	switch ip {
	case ipList[0]:
		return 0
	case ipList[1]:
		return 1
	case ipList[2]:
		return 2
	case ipList[3]:
		return 3
	case ipList[4]:
		return 4
	case ipList[5]:
		return 5
	case ipList[6]:
		return 6
	case ipList[7]:
		return 7
	case ipList[8]:
		return 8
	case ipList[9]:
		return 9
	default:
		return -1
	}
}

func vmToIP(vm int) string {
	switch vm {
	case 0:
		return ipList[0]
	case 1:
		return ipList[1]
	case 2:
		return ipList[2]
	case 3:
		return ipList[3]
	case 4:
		return ipList[4]
	case 5:
		return ipList[5]
	case 6:
		return ipList[6]
	case 7:
		return ipList[7]
	case 8:
		return ipList[8]
	case 9:
		return ipList[9]
	default:
		return ""
	}
}
