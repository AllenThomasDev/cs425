package main

import "fmt"

func opWrapper(execFilename string, isStateful bool, stateFilename string, isOutput bool, outputFilename string, logFilename string) {
	fmt.Println(execFilename, isStateful, stateFilename, isOutput, outputFilename, logFilename)
	fmt.Printf("Wrapping up the ops\n")
}