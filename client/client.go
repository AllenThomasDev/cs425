package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
)

type Args struct {
	Arg1, Arg2, Arg3 int
}

type Grep_Result struct {
	Matches []string
}

type Grep_Args struct {
	Line_Args []string
}

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:8880")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// grep start
	grep_reply := Grep_Result{}
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Enter your grep arguments to query log files:\ngrep ")
	var grep_args_line string
	grep_args_line, err = reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	grep_args := Grep_Args{strings.Split(grep_args_line, " ")}
	err = client.Call("Query.Grep", grep_args, &grep_reply)
	if err != nil {
		log.Fatal("client call failed:", err)
	}
	for _, line := range grep_reply.Matches {
		fmt.Println(line)
	}
}
