package main

import (
	"log"
	"net/rpc"
)

type Args struct {
	Arg1, Arg2, Arg3 int
}

type result struct {
	Test_Int int
}

type Grep_Result struct {
	Matches []string
}

type Grep_Args struct {
	Line_Args []string
}

func main() {
	reply := result{}
	args := Args{1, 2, 3}
	client, err := rpc.DialHTTP("tcp", "localhost:8880")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	err = client.Call("Arith.Add3", args, &reply)
	if err != nil {
		log.Fatal("client call failed:", err)
	}
	log.Printf("sum of 3 numbers: %d", reply.Test_Int)

	// grep start
	grep_reply := Grep_Result{}
	grep_args := Grep_Args{make([]string, 5)} // testing []string
	err = client.Call("Query.Grep", grep_args, &grep_reply)
	if err != nil {
		log.Fatal("client call failed:", err)
	}
}
