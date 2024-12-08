package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Rainstorm_tuple_t struct {
	Key string
	Value string
}

func convertStringToRT(strRT string) Rainstorm_tuple_t {
	parts := strings.Split(strRT, "~")
	if len(parts) == 1 {
		return Rainstorm_tuple_t{parts[0], ""}
	}
	return Rainstorm_tuple_t{parts[0], parts[1]}
}

func main() {
	args := os.Args
	strRT := args[1]

	rt := convertStringToRT(strRT)
	currVal, _ := strconv.Atoi(rt.Value)
	rt.Value = strconv.Itoa(currVal + 1);
	fmt.Print(rt.Key, "~", rt.Value)
}