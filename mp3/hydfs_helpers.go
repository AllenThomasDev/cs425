package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

func gen_merge_files(filename string, append_size int, num_clients int) {
	appString := ""
	for i := 0; i < append_size; i++ {
		appString = appString + "A"
	}

	fmt.Println("string generation finished")
	ns := 0
	for i := 0; i < num_clients * 1000; i++ {
		ts := time.Date(2024, time.November, 11, 11, 11, 11, ns, time.UTC).String()
		randFname := genRandomFileName()

		fmt.Println("writing file")

		writeFile(randFname, appString, "server")
		fmt.Println("file writing finished")

		aID := Append_id_t{1, ts};

		fileChannels[filename] <- aID
		aIDtoFile[filename][aID] = randFname
		ns++
	}
}

func periodicMerge() {
	for {
		routingTableMutex.RLock()
		ownedFiles := findOwnedFiles()
		routingTableMutex.RUnlock()
		logger.Println("Trying to run periodic merge")
		if len(ownedFiles) > 0 {
			randFile := ownedFiles[rand.Intn(len(ownedFiles))]
			logger.Printf("Running periodic merge on file %s\n", randFile)
			fileLogMutexes[randFile].Lock()
			forwardMerge(randFile)
			err := mergeFile(randFile, fileLogs[randFile])
			if err != nil {
				fmt.Printf("Error running periodic merge: %v\n", err)
				return;
			}
			fileLogMutexes[randFile].Unlock()
		}
		time.Sleep(time.Minute)
	}
}

var genRandomFileName = func() string {
	return(strconv.Itoa(int(rand.Int31())))
}

// checks file name for slashes and returns indices of slashes
func checkFileNameForSlashes(filename string) []int {
	slashIndices := make([]int, 0)
	for i := 0; i < len(filename); i++ {
		if filename[i] == '/' {
			slashIndices = append(slashIndices, i)
		}
	}
	return slashIndices
}

func removeSlashes(filename string, slashIndices []int) string {
	for i := 0; i < len(slashIndices); i++ {
		filename = filename[:i] + "`" + filename[i+1:]
	}
	return filename
}

func slashesToBackticks(filename string) string {
	btStr := ""
	for i := 0; i < len(filename); i++ {
		if filename[i] == '/' {
			btStr = btStr + "`"
		} else {
			btStr = btStr + string(filename[i])
		}
	}
	return btStr
}

func backticksToSlashes(filename string) string {
	slStr := ""
	for i := 0; i < len(filename); i++ {
		if filename[i] == '`' {
			slStr = slStr + "/"
		} else {
			slStr = slStr + string(filename[i])
		}
	}
	return slStr
}

func checkFileExists(localFileName string) bool {
	_, err := os.Stat(localFileName)
	if os.IsNotExist(err) {
		log.Printf("Error: Local file %s does not exist\n", localFileName)
		return false
	}
	return true
}

func checkFileOpens(localFileName string) bool {
	_, err := os.OpenFile(localFileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("Error opening local file\n")
		return false
	}
	return true
}

func writeFile(fileName string, fileContent string, writeTo string) error {
	// Open the file with O_CREATE and O_EXCL flags to ensure it fails if the file already exists
	file, err := os.OpenFile(writeTo + "/" + fileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			logger.Printf("File already exists\n")
			return err
		}
		return fmt.Errorf("error creating file %s: %v", fileName, err)
	}

	// Write the file content
	_, err = file.WriteString(fileContent)
	if err != nil {
		return fmt.Errorf("error writing to file %s: %v", fileName, err)
	}

	return nil
}

func appendFile(fileName string, fileContent string) (string, error) {
	exists := checkFileExists("server/" + fileName)
	if exists {
		randFileName := genRandomFileName()
		err := writeFile(randFileName, fileContent, "server")
		if err != nil {
			return "", fmt.Errorf("Error appending file %s: %v\n", fileName, err)
		}
		return randFileName, nil
	} else {
		return "", fmt.Errorf("NO EXIST\n")
	}
}

func readFileToString(localFileName string, writeFrom string) (string, error) {
	localFile, err := os.Open(writeFrom + "/" + localFileName)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("NO EXIST\n")
		}
		return "", err
	}
	defer localFile.Close()

	// Read the file content into a buffer
	fileBytes, err := io.ReadAll(localFile)
	if err != nil {
		return "", fmt.Errorf("error reading file %s into buffer: %v", localFileName, err)
	}

	// Create a message in the format "hyDFSFileName, fileContent"
	message := string(fileBytes)

	return message, nil
}

func readFileToMessageBuffer(localFileName string, writeFrom string) (string, error) {
	message, err := readFileToString(localFileName, writeFrom)
	if err != nil {
		logger.Printf("Error reading file %s into buffer: %v\n", localFileName, err)
		return "", err
	}
	return message, nil
}

func removeShards(filename string) error {
	for j := 0; j < len(fileLogs[filename]); j++ {
		// REMINDER: aIDtoFile uses filename, append id to give us randomized filename
		err := os.Remove("server/" + aIDtoFile[filename][fileLogs[filename][j]])
		if err != nil {
			return fmt.Errorf("Error removing shard %s: %v\n", aIDtoFile[filename][fileLogs[filename][j]], err)
		}
	}
	return nil
}

func removeFiles(repFiles []string) error {
	for i := 0; i < len(repFiles); i++ {
		logger.Printf("Removing file %s\n", repFiles[i])
		err := os.Remove("server/" + repFiles[i])
		if err != nil {
			logger.Printf("Error removing file %s: %v\n", repFiles[i], err)
			continue
		} else {
			err = removeShards(repFiles[i])
			if err != nil {
				return err
			}
		}
		// close channel, which will subsequently remove all file bookkeeping information
		close(fileChannels[repFiles[i]])
	}
	return nil
}

func appendRandomFile(fp *os.File, randomFilename string) error {
	fileContent, err := readFileToMessageBuffer(randomFilename, "server")
	if err != nil {
		return err
	}

	// Append the file content
	_, err = fp.WriteString(fileContent)
	if err != nil {
		return fmt.Errorf("error appending random file %s: %v", randomFilename, err)
	}

	return nil
}


func mergeFile(filename string, fileLog []Append_id_t) error {

	file, err := os.OpenFile("server/" + filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file %s does not exist", filename)
		}
		return fmt.Errorf("error opening file %s: %v", filename, err)
	}
	defer file.Close()

	var filesMerged int;
	for i := 0; i < len(fileLog); i++ {
		randomFilename := aIDtoFile[filename][fileLog[i]]
		err := appendRandomFile(file, randomFilename)
		if err != nil {
			return err
		}

		err = os.Remove("server/" + aIDtoFile[filename][fileLog[i]])
		if err != nil {
			return fmt.Errorf("Error removing shard %s: %v\n", aIDtoFile[filename][fileLog[i]], err)
		}

		delete(aIDtoFile[filename], fileLog[i])
		filesMerged++
	}

	// clear corresponding bookkeeping info as well
	fileLogs[filename] = fileLogs[filename][filesMerged:]
	
	return nil
}

func forwardMerge(hyDFSFilename string) {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()
	if len(successors) == 1 {
		return
	}
	
	args := ForwardedMergeArgs{hyDFSFilename, fileLogs[hyDFSFilename]}
	client, err := rpc.DialHTTP("tcp", vmToIP(successors[0]) + ":" + RPC_PORT)
	var reply string
	if err == nil {
		client.Call("HyDFSReq.ForwardedMerge", args, &reply)
	}
	
	if len(successors) > 2 {
		client, err := rpc.DialHTTP("tcp", vmToIP(successors[1]) + ":" + RPC_PORT)
		if err == nil {
			client.Call("HyDFSReq.ForwardedMerge", args, &reply)
		}
	}
}

func deleteFilesOnServer() {
  os.RemoveAll("./server")
  fmt.Println("Deleted server directory")
}

func deleteLocalLogs() {
	os.RemoveAll("./client/local_logs")
	fmt.Println("Removed residual RainStorm logs")
}
