package main

import (
	"fmt"
)

func addToHyDFS(ip string) {
	addToSuccessors(ipToVM(ip))
	addToRoutingTable(ipToVM(ip))
}

func removeFromHyDFS(ip string) {
	removeFromSuccessors(ipToVM(ip))
	removeFromRoutingTable(ipToVM(ip))
}

func addToSuccessors(hash int) {
	found, insertIndex := searchSuccessors(0, len(successors)-1, hash)
	if !found {
		if insertIndex > len(successors)-1 {
			successors = append(successors, hash)
		} else {
			lastElement := successors[len(successors)-1]
			if insertIndex != len(successors)-1 {
				copy(successors[insertIndex+1:], successors[insertIndex:])
			}
			successors = append(successors, lastElement)
			successors[insertIndex] = hash
		}
	}
	printSuccessors()
}

func removeFromSuccessors(hash int) {
	found, index := searchSuccessors(0, len(successors)-1, hash)
	if found {
		successors = append(successors[0:index], successors[index+1:]...)
	}
	printSuccessors()
}

func addToRoutingTable(hash int) {
	// routing table is current if new hash maps to respective VM
	if !(routingTable[hash] == vmToIP(hash)) {
		routingTable[hash] = vmToIP(hash)

		// change entries in routing table between new node and previous node
		nextLowest := mod((hash - 1), MACHINES_IN_NETWORK)

		for {
			// fmt.Printf("%d\n", nextLowest)
			if nextLowest == hash || routingTable[nextLowest] == vmToIP(nextLowest) {
				break
			}
			routingTable[nextLowest] = vmToIP(hash)

			nextLowest = mod((nextLowest - 1), MACHINES_IN_NETWORK)
		}
	}
	printRoutingTable()
}

func removeFromRoutingTable(hash int) {
	if routingTable[hash] == vmToIP(hash) {

		nextIP := routingTable[(hash+1)%MACHINES_IN_NETWORK]
		routingTable[hash] = nextIP

		nextLowest := mod((hash - 1), MACHINES_IN_NETWORK)

		for {
			if routingTable[nextLowest] == vmToIP(nextLowest) || nextLowest == hash {
				break
			}
			routingTable[nextLowest] = nextIP
			nextLowest = mod((nextLowest - 1), MACHINES_IN_NETWORK)
		}
	}
	printRoutingTable()
}

func searchSuccessors(low int, high int, hash int) (bool, int) {
	if low > high {
		return false, low
	}

	med := (high + low) / 2
	if successors[med] == hash {
		return true, med
	}

	if clockwiseDistance(currentVM, successors[med]) < clockwiseDistance(currentVM, hash) {
		return searchSuccessors(med+1, high, hash)
	} else {
		return searchSuccessors(low, med-1, hash)
	}
}

func clockwiseDistance(start int, cw int) int {
	if cw < start {
		return (cw + MACHINES_IN_NETWORK) - start
	} else {
		return cw - start
	}
}

func printRoutingTable() {
	for k, v := range routingTable {
		fmt.Printf("Hash %d maps to VM %d\n", k, ipToVM(v))
	}
}

func printSuccessors() {
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
