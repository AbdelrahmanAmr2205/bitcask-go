package main

import (
	"fmt"
	"log"
	"os"

	"github.com/AbdelrahmanAmr2205/bitcask-go"
)

func main() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	db, err := bitcask.InitDB(homeDir+"/bitcask/test", 4194304, 0, 0)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Put("foo", []byte("bar"))
	if err != nil {
		log.Fatal(err)
	}

	val, err := db.Get("foo")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(val))
}
