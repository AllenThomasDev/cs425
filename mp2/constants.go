package main

import (
	"time"
	"github.com/hashicorp/golang-lru/v2/expirable"
)
// note this is hashable, so we can use it as a key in a map
type Append_id_t struct {
	Vm int
	Timestamp string
}

type member_type_t int
const (
	NEW_MEMBER member_type_t = iota
	OLD_MEMBER
)

var cache = expirable.NewLRU[string, string](5, nil, 5*time.Second)

type ack_type_t int
const (
	GOOD_ACK ack_type_t = iota
	ERROR_ACK
	TIMEOUT_ACK
)

var (
	MACHINES_IN_NETWORK = 10
	PANIC_ON_ERROR		= 0
	RPC_PORT			= "2233"
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
