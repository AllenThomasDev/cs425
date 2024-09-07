package main

import (
	"bufio"
	"bytes"
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
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

	curr_dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// iterate

	filepath.Walk(curr_dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		if !info.IsDir() { // walk includes the root so make sure to exclude that
			var f *os.File

			f, err = os.Open(path)
			if err != nil {
				log.Fatal(err)
			}

			file_scanner := bufio.NewScanner(f)
			file_scanner.Split(bufio.ScanLines)                        // there might be a limit on the line length to check for
			args.Line_Args = append(args.Line_Args, info.Name())       // grep only works with files
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
		}
		return nil
	})
	return nil
}

func main() {
	err := os.Chdir("./logs")
	if err != nil {
		log.Fatal(err)
	}
	grep := new(Query)
	rpc.Register(grep)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":8880")
	if err != nil {
		log.Fatal("failed to connect to port 8880")
	}
	http.Serve(l, nil)
}
