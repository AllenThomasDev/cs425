package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
)

type Grep_Result struct {
	Matches []string
}

type Grep_Args struct {
	Line_Args []string
}

type Query int

func (t *Query) Grep(args *Grep_Args, result *Grep_Result) error {
	if len(args.Line_Args) == 0 {
		return errors.New("no arguments passed to grep")
	}

	f, err := os.Open("simple_log.txt") // may or may not work
	if err != nil {
		log.Fatal(err)
	}

	args.Line_Args = append(args.Line_Args, "simple_log.txt")  // grep only works with files
	args.Line_Args = append([]string{"-n"}, args.Line_Args...) // prepend to add line numbers to output
	var cmd []byte
	cmd, err = exec.Command("grep", args.Line_Args...).Output()
	if err != nil {
		log.Fatal("grep command failed", err)
	}
	lines := bytes.Split(cmd, []byte("\n")) // split output on newlines to get each line of the file

	// if the result of grep results in matches
	for _, line := range lines {
		if len(line) != 0 {
			result.Matches = append(result.Matches, string(line))
		}
	}

	err = f.Close()
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

// TODO: add a call to os.exec to start 10 servers without a port number as an argument in a loop
// TODO: consider adding a list that holds available port numbers to avoid trying to query failed port numbers
//
//	on a failure, this would be sent to the remaining servers to then update the list
func main() {
	fmt.Println(len(os.Args))
	for _, arg := range os.Args {
		fmt.Println(arg)
	}
	if len(os.Args) != 2 {
		log.Fatal("Server only requires a valid port number from 8880 to 8889 as an argument")
	} else if port, port_err := strconv.Atoi(os.Args[1]); port_err != nil || port < 8880 || port > 8889 {
		log.Fatal("Server only requires a valid port number from 8880 to 8889 as an argument")
	}
	port_number := os.Args[1]
	err := os.Chdir("./logs")
	if err != nil {
		log.Fatal(err)
	}
	grep := new(Query)
	rpc.Register(grep)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+port_number)
	if err != nil {
		log.Fatal("failed to connect to port " + port_number)
	}
	http.Serve(l, nil)
}
