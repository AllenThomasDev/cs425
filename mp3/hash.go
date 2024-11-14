package main

import (
	"crypto/sha1"
	"math/big"
)

func hash(fileName string, maxHash int) int {
	hash := sha1.Sum([]byte(fileName))
	hashInt := new(big.Int).SetBytes(hash[:])
	nodeIndex := new(big.Int).Mod(hashInt, big.NewInt(int64(maxHash)))
	return int(nodeIndex.Int64())
}
