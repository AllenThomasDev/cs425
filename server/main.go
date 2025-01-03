package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os/exec"
)

type GrepRequest struct {
	Pattern string `json:"pattern"`
	Path    string `json:"path"`
}

type GrepResponse struct {
	VM     string `json:"vm"`     // Identifier for the VM
	Output string `json:"output"` // Grep output
	Error  string `json:"error"`  // Any error encountered
}

func main() {
	// Start listening on a specific port for client commands
	listener, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatal("Error starting Server:", err)
	}
	defer listener.Close()

	fmt.Println("Server listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Decode the incoming grep request
	var req GrepRequest
	err := json.NewDecoder(conn).Decode(&req)
	if err != nil {
		log.Println("Error decoding request:", err)
		return
	}

	// Execute grep locally
	output, err := executeGrep(req.Pattern, req.Path)
	resp := GrepResponse{
		VM:     getVMIdentifier(), // This function will return the VM's identifier, such as "vm1"
		Output: output,
	}

	if err != nil {
		resp.Error = err.Error()
	}

	// Send the grep result back to the client
	err = json.NewEncoder(conn).Encode(resp)
	if err != nil {
		log.Println("Error sending response:", err)
	}
}

// executeGrep runs the grep command locally on ~/*.log files

func executeGrep(pattern string, file_path string) (string, error) {
	cmd := exec.Command("bash", "-c", fmt.Sprintf("grep %s   %s", pattern, file_path))
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// getVMIdentifier returns a simple identifier for the VM (e.g., "vm1")
func getVMIdentifier() string {
	// You can change this to return the actual hostname or a hardcoded name
	hostname, err := exec.Command("hostname").Output()
	if err != nil {
		log.Println("Error getting hostname:", err)
		return "unknown-vm"
	}
	return string(hostname)
}
