package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
	err := os.Chdir("./logs")
	if err != nil {
		log.Fatal(err)
	}
	var curr_dir string
	curr_dir, err = os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// iterate

	filepath.Walk(curr_dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		// TODO: change to grep files instead of just printing the file name
		if !info.IsDir() { // walk includes the root so make sure to exclude that
			var f *os.File

			f, err = os.Open(path)
			if err != nil {
				log.Fatal(err)
			}
			line_number := 1

			file_scanner := bufio.NewScanner(f)
			file_scanner.Split(bufio.ScanLines) // there might be a limit on the line length to check for

			for file_scanner.Scan() {
				fmt.Println(strconv.Itoa(line_number) + ": " + file_scanner.Text())
				line_number++
			}
			fmt.Printf("File name: %s\n", info.Name())
			err = f.Close()
			if err != nil {
				log.Fatal(err)
			}
		}
		return nil
	})
	// TODO: modify to grep the file in the Walk function
	exec.Command("grep")
	return nil
}

func main() {
	arith := new(Arith)
	rpc.Register(arith)
	grep := new(Query)
	rpc.Register(grep)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":8880")
	if err != nil {
		log.Fatal("failed to connect to port 8880")
	}
	http.Serve(l, nil)
}
