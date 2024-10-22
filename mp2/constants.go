package main

var (
	MATT_VMS            = 0 // set to 0 to use 54XX VMs
	MACHINES_IN_NETWORK = 10
	introducerIP        = ""
	ipList              []string // initialized based on MATT_VMS
	currentVM           int      // also initialized based on MATT_VMS

	currentIP = GetOutboundIP().String()

	// since our network is just our machine at first, our routing table is just us to start
	routingTable = map[int]string{
		0: currentIP,
		1: currentIP,
		2: currentIP,
		3: currentIP,
		4: currentIP,
		5: currentIP,
		6: currentIP,
		7: currentIP,
		8: currentIP,
		9: currentIP,
	}

	// list of all nodes in network clockwise from current node
	successors = make([]int, 0)
)

func initIPList() {
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
	currentVM = ipToVM(currentIP)
}
