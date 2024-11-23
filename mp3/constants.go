package main

import (
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

type Rainstorm_tuple_t struct {
	Key string
	Value string
}

type task_addr_t struct {
	VM int
	port string 
}

type topology_entry_t struct {
	Layer int
	Hash int
}

type Task_type_t int
const (
	SOURCE Task_type_t = iota
	OP
)

// note this is hashable, so we can use it as a key in a map
type Append_id_t struct {
	Vm int
	Timestamp string
}

type source_data_t struct {
	line string
	lineNum int
}

type member_type_t int
const (
	NEW_MEMBER member_type_t = iota
	OLD_MEMBER
)

// FileChunkInfo contains the line and character information for file chunks
type FileChunkInfo struct {
	StartLines     []int
	StartChars     []int
	LinesPerSource []int
}

type fileAnalysis struct {
	lineCount   int
	charsAtLine []int
}

var cache = expirable.NewLRU[string, string](0, nil, 15000*time.Second)
var UIDCache = make(map[int]bool)

type ack_type_t int
const (
	GOOD_ACK ack_type_t = iota
	ERROR_ACK
	TIMEOUT_ACK
)

var (
	MACHINES_IN_NETWORK = 10
	PANIC_ON_ERROR		= 0
	RAINSTORM_LAYERS	= 3		// source, op1, op2 -> we will always have 3 layers for rainstorm operation
	RPC_PORT			= "2233"
	SCHEDULER_PORT		= "2234"
	introducerIP        = "172.22.94.178"
	ipList              = []string {
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
	selfIP = GetOutboundIP().String()
	currentVM		= ipToVM(selfIP)
	routingTable	= map[int]int {
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

	// list of all nodes in network clockwise from current node INCLUDING CURRENT NODE
	successors = []int {currentVM}
)

var (
	LEADER_ID	= 0
)
