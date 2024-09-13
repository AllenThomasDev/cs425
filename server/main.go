package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
)

var vmIPs = map[string]string{
	"vm1":  "172.22.94.178",
	"vm2":  "172.22.156.179",
	"vm3":  "172.22.158.179",
	"vm4":  "172.22.94.179",
	"vm5":  "172.22.156.180",
	"vm6":  "172.22.158.180",
	"vm7":  "172.22.94.180",
	"vm8":  "172.22.156.181",
	"vm9":  "172.22.158.181",
	"vm10": "172.22.94.181",
}

// GrepRequest struct for sending grep queries to clients
type GrepRequest struct {
	Pattern string `json:"pattern"`
}

// GrepResponse struct for receiving responses from clients
type GrepResponse struct {
	VM     string `json:"vm"`     // Identifier for the VM
	Output string `json:"output"` // Grep output
	Error  string `json:"error"`  // Any error encountered
}

func main() {
	// Start listening on a specific port for client connections
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("Error starting server:", err)
	}
	defer listener.Close()

	fmt.Println("Server listening on :8080")

	// Ask the user for the grep pattern
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter grep pattern: ")
	pattern, _ := reader.ReadString('\n')

	// Remove newline from pattern
	pattern = pattern[:len(pattern)-1]

	// Broadcast the grep request to all VMs
	broadcastGrepRequest(pattern)

	// Keep listening for client responses
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

// broadcastGrepRequest sends grep queries to all VMs
func broadcastGrepRequest(pattern string) {
	var wg sync.WaitGroup

	for vm, ip := range vmIPs {
		wg.Add(1)

		go func(vm, ip string) {
			defer wg.Done()

			// Connect to the client VM
			conn, err := net.Dial("tcp", ip+":8081")
			if err != nil {
				log.Printf("Error connecting to VM %s (%s): %v\n", vm, ip, err)
				return
			}
			defer conn.Close()

			// Prepare the grep request
			req := GrepRequest{
				Pattern: pattern,
			}

			// Send the request as JSON
			err = json.NewEncoder(conn).Encode(req)
			if err != nil {
				log.Printf("Error sending request to VM %s (%s): %v\n", vm, ip, err)
				return
			}

			// Read and print the client's response
			var resp GrepResponse
			err = json.NewDecoder(conn).Decode(&resp)
			if err != nil {
				log.Printf("Error receiving response from VM %s (%s): %v\n", vm, ip, err)
				return
			}

			if resp.Error != "" {
				log.Printf("Error from VM %s: %s\n%s\n", vm, ip, resp.Error)
			} else {
				log.Printf("Output from VM %s: %s\n\n%s\n", vm, ip, resp.Output)
			}
		}(vm, ip)
	}

	wg.Wait()
	fmt.Println("All grep requests completed.")
}

// handleConnection handles incoming client connections (for client responses)
func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read the incoming grep query request
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var req GrepRequest
		err := json.Unmarshal(scanner.Bytes(), &req)
		if err != nil {
			log.Println("Error decoding request:", err)
			continue
		}

		// Execute the grep command locally
		output, err := executeGrep(req.Pattern)

		resp := GrepResponse{Output: output}
		if err != nil {
			resp.Error = err.Error()
		}

		// Send the grep response back to the server
		json.NewEncoder(conn).Encode(resp)
	}
}

// executeGrep runs the grep command locally on the machine
func executeGrep(pattern string) (string, error) {
	// Always search in ~/*.log
	cmd := exec.Command("bash", "-c", fmt.Sprintf("grep '%s' ~/*.log", pattern))
	output, err := cmd.CombinedOutput()
	return string(output), err
}
