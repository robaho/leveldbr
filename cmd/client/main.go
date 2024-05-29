package main

import (
	"bytes"
	"flag"
	"log"
	"time"

	"github.com/robaho/leveldbr/client"
)

func main() {
	addr := flag.String("addr", "localhost:8501", "set the remote database address")
	dbname := flag.String("db", "main", "set the remote database name")
	create := flag.Bool("c", true, "create if needed")
	timeout := flag.Int("t", 5, "number of seconds before timeout")

	flag.Parse()

	db, err := client.Open(*addr, *dbname, *create, *timeout)
	if err != nil {
		log.Fatal("error with open ",err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		log.Fatal("error with put ",err)
	}

	value, err := db.Get([]byte("mykey"))
	if err != nil {
		log.Fatal("error with get ",err)
	}
	if !bytes.Equal(value, []byte("myvalue")) {
		log.Fatal("unexpected value")
	}

	time.Sleep(time.Second * 2)

	err = db.Close()
	if err != nil {
		log.Fatal("error with close ",err)
	}
}
