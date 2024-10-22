package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

func create(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: create localfilename HyDFSfilename")
	} else {
		localFile, err := os.OpenFile(args[0], os.O_RDONLY, 0644)
		if err != nil {
			fmt.Printf("Error opening local file\n")
		}

		// @TODO right now all writes go to introducer, this is just for testing, wwill fix this
		address := net.JoinHostPort(introducerIP, IO_PORT)
		conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		if err != nil {
			fmt.Printf("Error connecting to host %d: %v\n", hash(args[0]), err)
			return
		}
		defer conn.Close()

		fStats, err := localFile.Stat()
		if err != nil {
			fmt.Printf("Error retrieving size of file: %v\n", err)
		}

		fileBytes := make([]byte, fStats.Size())
		readLen, err := localFile.Read(fileBytes)
		if err != nil || int64(readLen) != fStats.Size() {
			fmt.Printf("Error reading file into buffer: %v\n", err)
		}

		message := make([]byte, 0)
		message = append(message, []byte(args[0]+",")...)
		message = append(message, fileBytes...)

		bytesSent, err := conn.Write(message)
		if err != nil {
			fmt.Printf("Error sending file to host %d: %v\n", hash(args[0]), err)
		} else {
			fmt.Printf("Sent new file to %s: %s (message size: %d bytes)", vmToIP(hash(args[0])), message, bytesSent)
		}
	}
}

func listenFileIo() {
	// open connection on CREATE_PORT
	listener, err := net.Listen("tcp", ":"+string(IO_PORT))
	if err != nil {
		fmt.Printf("Error opening IO Port: %v\n", err)
		return
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			var newFile *os.File
			filename := ""
			buf := make([]byte, 1024)
			for {
				n, err := c.Read(buf)
				if err != nil {
					// read until end of sent data
					if err == io.EOF {
						break
					} else {
						fmt.Printf("Error reading from connection: %v\n", err)
					}
				}
				// if we haven't found a filename yet, extract it from the sent data
				if filename == "" {
					// split into two substrings
					parts := strings.SplitAfterN(string(buf[:n]), ",", 2)
					if len(parts) < 2 {
						fmt.Printf("Error: data received does not contain filename/file data")
						return
					}
					newFile, err = os.OpenFile(parts[0], os.O_CREATE|os.O_WRONLY, 0644)
					defer newFile.Close()
					if err != nil {
						fmt.Printf("Error opening file %s: %v\n", parts[0], err)
					}
					newFile.Write([]byte(parts[1]))
				} else {
					newFile.Write(buf[:n])
				}
			}
		}(conn)
	}
}

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
