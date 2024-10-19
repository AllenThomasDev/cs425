package main

import (
	"fmt"
)

var (
	// set to 0 to use 54XX VMs
	MATT_VMS = 1
	
	MACHINES_IN_NETWORK = 10
	coordinatorPort = 9000
	// initialized based on MATT_VMS
	ipList []string
	
	// since our network is just our machine at first, our routing table is just us to start
	routingTable = map[int]string {
		1:	GetOutboundIP().String(),
		2:	GetOutboundIP().String(),
		3:	GetOutboundIP().String(),
		4:	GetOutboundIP().String(),
		5:	GetOutboundIP().String(),
		6:	GetOutboundIP().String(),
		7:	GetOutboundIP().String(),
		8:	GetOutboundIP().String(),
		9:	GetOutboundIP().String(),
		10:	GetOutboundIP().String(),
	}
)

func create(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: create localfilename HyDFSfilename")
	} else {
		fmt.Println("Good command!")
		// localFile, err := os.OpenFile(args[0], os.O_RDONLY, 0644)
		
		// if err != nil {
		// 	fmt.Printf("Error opening local file: %v\n")
		// }

		// hash(args[0])
		// conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		// if err != nil {
		// 	return
		// }
		// defer conn.Close()

		// messageBytes := []byte(message)
		// bytesSent, err := conn.Write(messageBytes)
		// if err != nil {
		// } else {
		// 	logger.Printf("Sent TCP message to %s: %s (size: %d bytes)", targetIP, message, bytesSent)
		// }
		// address := net.JoinHostPort(hash())
	}
}

// iterate through member list, determining IP of coordinator node for file

func updateRoutingTable(hash int) {
	// routing table is current if new hash maps to respective VM
	if !(routingTable[hash] == vmToIP(hash)) {
		routingTable[hash] = vmToIP(hash)

		var nextLowest int
		if hash - 1 < 1 {
			nextLowest = MACHINES_IN_NETWORK
		} else {
			nextLowest = hash - 1
		}

		for ;; {
			fmt.Printf("%d\n", nextLowest)
			if nextLowest == hash || routingTable[nextLowest] == vmToIP(nextLowest) {
				break
			}
			routingTable[nextLowest] = vmToIP(hash)

			if nextLowest - 1 < 1 {
				nextLowest = MACHINES_IN_NETWORK
			} else {
				nextLowest = nextLowest - 1
			}
		}
	}
	printRoutingTable()
}

func printRoutingTable () {
	for k, v := range routingTable {
		fmt.Printf("Hash %d maps to VM %d\n", k, ipToVM(v));
	}
}

func initIPList () {
	if MATT_VMS == 1 {
		ipList = []string {
			"172.22.94.188",
			"172.22.156.189",
			"172.22.158.189",
			"172.22.94.189",
			"172.22.156.190",
			"172.22.158.190",
			"172.22.94.190",
			"172.22.156.191",
			"172.22.158.191",
			"172.22.94.191",
		}
	} else {
		ipList = []string {
			"172.22.94.178",
			"172.22.156.179",
			"172.22.158.179",
			"172.22.94.179",
			"172.22.156.180",
			"172.22.158.180",
			"172.22.94.180",
			"172.22.156.181",
			"172.22.158.181",
			"172.22.94.181",
		}
	} 
}

func ipToVM (ip string) int {
	switch (ip) {
	case ipList[0]:
		return 1
	case ipList[1]:
		return 2
	case ipList[2]:
		return 3
	case ipList[3]:
		return 4
	case ipList[4]:
		return 5
	case ipList[5]:
		return 6
	case ipList[6]:
		return 7
	case ipList[7]:
		return 8
	case ipList[8]:
		return 9
	case ipList[9]:
		return 10
	default:
		return -1
	}
}

func vmToIP (vm int) string {
	switch (vm) {
	case 1:
		return ipList[0]
	case 2:
		return ipList[1]
	case 3:
		return ipList[2]
	case 4:
		return ipList[3]
	case 5:
		return ipList[4]
	case 6:
		return ipList[5]
	case 7:
		return ipList[6]
	case 8:
		return ipList[7]
	case 9:
		return ipList[8]
	case 10:
		return ipList[9]
	default:
		return ""
	}
}