package main

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
)

func checkFileExists(localFileName string) bool {
	_, err := os.Stat(localFileName)
	if os.IsNotExist(err) {
		fmt.Printf("Error: Local file %s does not exist\n", localFileName)
		return false
	}
	return true
}

func checkFileOpens(localFileName string) bool {
	_, err := os.OpenFile(localFileName, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening local file\n")
		return false
	}
	return true
}

func writeFile(fileName string, fileContent string, writeTo string) error {
	// Open the file with O_CREATE and O_EXCL flags to ensure it fails if the file already exists
	file, err := os.OpenFile(writeTo + "/" + fileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			fmt.Printf("File already exists\n")
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
		//@TODO:maybe move appends to another folder? it can cause confusion when using "store" command, 
    //or we could add a check in store to ignore purely numeric file entires 
		err := writeFile(randFileName, fileContent, "server")
		if err != nil {
			fmt.Printf("ERROR IN APPEND\n")
			return "", fmt.Errorf("Error appending file %s: %v\n", fileName, err)
		}
		return randFileName, nil
	} else {
		fmt.Printf("ERROR IN APPEND\n")
		return "", fmt.Errorf("Error: file does not exist\n")
	}
}

func readFileToString(localFileName string, writeFrom string) (string, error) {
	localFile, err := os.Open(writeFrom + "/" + localFileName)
	if err != nil {
		return "", fmt.Errorf("error opening local file %s: %v", localFileName, err)
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
		fmt.Printf("Error reading file %s into buffer: %v\n", localFileName, err)
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
		fmt.Printf("Removing file %s\n", repFiles[i])
		err := os.Remove("server/" + repFiles[i])
		if err != nil {
			fmt.Printf("Error removing file %s: %v\n", repFiles[i], err)
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

	for i := 0; i < len(fileLog); i++ {
		randomFilename := aIDtoFile[filename][fileLog[i]]
		err := appendRandomFile(file, randomFilename)
		if err != nil {
			return err
		}
	}
	
	// remove all shards after writing them
	err = removeShards(filename)
	if err != nil {
		return err
	}

	// clear corresponding bookkeeping info as well
	fileLogs[filename] = make([]Append_id_t, 0)
	aIDtoFile[filename] = make(map[Append_id_t]string)
	
	return nil
}

func forwardMerge(hyDFSFilename string) {
	successorsMutex.RLock()
	defer successorsMutex.RUnlock()
	if len(successors) == 1 {
		return
	}
	
	args := ForwardedMergeArgs{hyDFSFilename, fileLogs[hyDFSFilename]}
	client, _ := rpc.DialHTTP("tcp", vmToIP(successors[0]) + ":" + RPC_PORT)

	var reply string
	client.Call("HyDFSReq.ForwardedMerge", args, &reply)
	
	if len(successors) > 2 {
		client, _ := rpc.DialHTTP("tcp", vmToIP(successors[1]) + ":" + RPC_PORT)
		client.Call("HyDFSReq.ForwardedMerge", args, &reply)
	}
}

func deleteFilesOnServer() {
  os.RemoveAll("./server")
  fmt.Println("Deleted server directory")
}
