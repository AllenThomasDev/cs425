package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
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

	// grep start
	grep_reply := Grep_Result{}
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Enter your grep arguments to query log files:\ngrep ")
	grep_args_line, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	grep_args_line = strings.Trim(grep_args_line, "\n") // remove the \n to avoid creating an empty string on Split
	grep_args := Grep_Args{strings.Split(grep_args_line, " ")}

	for i := 0; i < 10; i++ {
		client, err := rpc.DialHTTP("tcp", "localhost:888"+strconv.Itoa(i))
		if err != nil {
			log.Println("dialing:", err)
			continue
		}
		err = client.Call("Query.Grep", grep_args, &grep_reply) // consider using Go instead of call
		if err != nil {
			log.Fatal("client call failed:", err)
		}
		for _, line := range grep_reply.Matches {
			fmt.Println(line)
		}
	}
}
