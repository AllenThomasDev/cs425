package main

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Result struct {
	Test_Int int
}

type Args struct {
	Arg1, Arg2, Arg3 int
}

type Arith int

func (t *Arith) Add3(args *Args, result *Result) error {
	if args.Arg1 < 0 || args.Arg2 < 0 || args.Arg3 < 0 {
		return errors.New("negative arguments")
	}
	result.Test_Int = args.Arg1 + args.Arg2 + args.Arg3
	return nil
}

func main() {
	arith := new(Arith)
	rpc.Register(arith)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":8880")
	if err != nil {
		log.Fatal("failed to connect to port 8880")
	}
	http.Serve(l, nil)
}
