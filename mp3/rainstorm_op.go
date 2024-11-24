package main

import "fmt"

var (
	portToOpData = make(map[string]OpArgs)
)

func opWrapper(OA OpArgs, appPort string) {
	fmt.Printf("Wrapping up the ops\n")
	// maintain state so we can use a common wrapper for stateful and stateless operators (or we could just break them up)
	portToOpData[appPort] = OA
}
