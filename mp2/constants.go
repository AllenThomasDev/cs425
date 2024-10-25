package main

import "time"

// this is slightly goofy, but the aID needs to persist after write to file while the fileContent doesn't
type append_log_t struct {
	aID append_id_t // ID of append
	fileContent string // content to be written in append
}

type append_info_t struct {
	aID append_id_t // 
	fileIndex int64
}

// note this is hashable, so we can use it as a key in a map
type append_id_t struct {
	vm int
	timestamp string
}

type member_type_t int
const (
	NEW_MEMBER member_type_t = iota
	OLD_MEMBER
)

type ack_type_t int
const (
	GOOD_ACK ack_type_t = iota
	ERROR_ACK
	TIMEOUT_ACK
)

var (
	MATT_VMS            = 1 // set to 0 to use 54XX VMs
	MACHINES_IN_NETWORK = 10
	HYDFS_TIMEOUT		= 10 * time.Second
	PANIC_ON_ERROR		= 0
	introducerIP        = ""
	ipList              []string // initialized based on MATT_VMS
	currentVM           int      // also initialized based on MATT_VMS

	selfIP = GetOutboundIP().String()

	// since our network is just our machine at first, our routing table is just us to start
	routingTable	map[int]int

	// list of all nodes in network clockwise from current node INCLUDING CURRENT NODE
	successors = make([]int, 0)
)

func initHyDFS() {
	if MATT_VMS == 1 {
		ipList = []string{
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
		introducerIP = "172.22.94.188"
	} else {
		ipList = []string{
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
		introducerIP = "172.22.94.178"
	}

	currentVM = ipToVM(selfIP)
	routingTable = map[int]int {
		0: currentVM,
		1: currentVM,
		2: currentVM,
		3: currentVM,
		4: currentVM,
		5: currentVM,
		6: currentVM,
		7: currentVM,
		8: currentVM,
		9: currentVM,
	}
	successors = append(successors, currentVM)
}
