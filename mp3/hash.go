package main

import (
	"crypto/sha1"
	"math/big"
)

func hash(fileName string) int {
	hash := sha1.Sum([]byte(fileName))
	hashInt := new(big.Int).SetBytes(hash[:])
	nodeIndex := new(big.Int).Mod(hashInt, big.NewInt(10))
	return int(nodeIndex.Int64())
}
