package main

import (
	"fmt"
	"log"
	"os/exec"
)

func main() {
	opOut, err := exec.Command("./filterPosts", "shit:-9822752.01226842,4887653.93470103,1,Streetname - Mast Arm,\"16\"\" X 42\"\"\", ,Traffic Signal Mast Arm, ,Streetname, ,D3-1,Champaign,1,,AERIAL,L,Mercury Dr,1,,{14F48419-04A7-4932-A850-884CA5DC67FA}", "Streetname").Output()
	if err != nil {
		fmt.Printf("CRAP\n")
		log.Fatal(err)
	}
	fmt.Print(string(opOut))
}