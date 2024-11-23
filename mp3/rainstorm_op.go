package main

import "fmt"

var (
	portToOpData = make(map[int]OpArgs)
)

func opWrapper(OA OpArgs, appPort int) {
	fmt.Printf("Wrapping up the ops\n")
	portToOpData[appPort] = OA
}
