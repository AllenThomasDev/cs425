package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

var OBJECTID_COLUMN = 2
var SIGNTYPE_COLUMN = 3

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

func splitOnCommas(csvals string) []string {
	csvReader := csv.NewReader(strings.NewReader(csvals))
	rowVals, _ := csvReader.Read()
	return rowVals
}

func chooseColumns(rt Rainstorm_tuple_t) Rainstorm_tuple_t {
	parts := splitOnCommas(rt.Value)
	return Rainstorm_tuple_t{parts[OBJECTID_COLUMN] + "," + parts[SIGNTYPE_COLUMN], ""}
}

func main() {
	args := os.Args
	rt := convertStringToRT(args[1])
	retRT := chooseColumns(rt)
	
	fmt.Print(retRT.Key, "~", retRT.Value)
}