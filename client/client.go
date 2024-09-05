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
}
