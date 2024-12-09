package main

import (
	"fmt"
	"os"
	"strings"
)

var SIGNPOST_COLUMN = 6
var CATEGORY_COLUMN = 8
var FILTERED		= "this tuple has been filtered out"

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

func filterLine(rt Rainstorm_tuple_t, pattern string) Rainstorm_tuple_t {
	if strings.Contains(rt.Value, pattern) {
		return rt
 	} else {
		return Rainstorm_tuple_t{FILTERED, FILTERED}
	}
}

func main() {
	//fmt.Print("filterPosts called!\n")
	args := os.Args
	// for arg := range args {
	// 	fmt.Println(args[arg])
	// }
	rt := convertStringToRT(args[1])
	//fmt.Print("RT in filter: " + rt.Key + ":" + rt.Value + "\n")
	pattern := args[2]
	retRT := filterLine(rt, pattern)
	
	fmt.Print(retRT.Key, "~", retRT.Value)
}