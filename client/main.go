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
	"time"
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

type GrepRequest struct {
	Pattern string `json:"pattern"`
}

type GrepResponse struct {
	VM     string `json:"vm"`
	Output string `json:"output"`
	Error  string `json:"error"`
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("Error starting client:", err)
	}
	defer listener.Close()

	fmt.Println("Client listening on :8080")

	// Continuously ask the user for grep patterns
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter grep pattern (or 'exit' to quit): ")
		pattern, _ := reader.ReadString('\n')
		pattern = pattern[:len(pattern)-1]

		if pattern == "exit" {
			fmt.Println("Exiting.")
			break
		}

		startTime := time.Now()
		broadcastGrepRequest(pattern)
		fmt.Printf("Total time for request: %v\n", time.Since(startTime))
	}
}

func broadcastGrepRequest(pattern string) {
	var wg sync.WaitGroup

	for vm, ip := range vmIPs {
		wg.Add(1)

		go func(vm, ip string) {
			defer wg.Done()

			conn, err := net.Dial("tcp", ip+":8081")
			if err != nil {
				log.Printf("Error connecting to VM %s (%s): %v\n", vm, ip, err)
				return
			}
			defer conn.Close()

			req := GrepRequest{Pattern: pattern}

			err = json.NewEncoder(conn).Encode(req)
			if err != nil {
				log.Printf("Error sending request to VM %s (%s): %v\n", vm, ip, err)
				return
			}

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

func handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var req GrepRequest
		err := json.Unmarshal(scanner.Bytes(), &req)
		if err != nil {
			log.Println("Error decoding request:", err)
			continue
		}

		startTime := time.Now()
		output, err := executeGrep(req.Pattern)

		resp := GrepResponse{Output: output}
		if err != nil {
			resp.Error = err.Error()
		}

		json.NewEncoder(conn).Encode(resp)

		// Measure the time taken for this grep execution
		fmt.Printf("Time taken for grep on VM: %v\n", time.Since(startTime))
	}
}

func executeGrep(pattern string) (string, error) {
	cmd := exec.Command("bash", "-c", fmt.Sprintf("grep '%s' ~/*.log", pattern))
	output, err := cmd.CombinedOutput()
	return string(output), err
}
