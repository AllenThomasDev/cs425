package main

func addFileToCache(filename string, contents string) {
	logger.Printf("File '%s': was cached\n", filename)
	cache.Add(filename, contents)
}

func readFileFromCache(filename string) (string, bool) {
	contents, ok := cache.Get(filename)
	return contents, ok
}

func removeFileFromCache(filename string) {
	cache.Remove(filename)
}
