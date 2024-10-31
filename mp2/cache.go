package main

import (
	"fmt"
)

func addFileToCache(filename string, contents string) {
	fmt.Printf("File '%s': was cached\n", filename)
	cache.Add(filename, contents)
}

func readFileFromCache(filename string) (string, bool) {
	contents, ok := cache.Get(filename)
	return contents, ok
}
