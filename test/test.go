package main

import (
	"fmt"
	"log"
	"os"
)

var (
	newFile *os.File
	err     error
)

func main() {

	str, _ := os.Getwd()
	fmt.Println(str)

	newFile, err = os.Create("/home/clone/zhuMQ/test/test1.txt")
	if err != nil {
		log.Fatal(err)
	}

	log.Println(newFile)

	newFile.Close()
}
