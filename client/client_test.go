package client_test

import (
	"github.com/robaho/leveldb"
	"github.com/robaho/leveldbr/client"
	"log"
	"testing"
)

var addr = "localhost:8501"
var dbname = "main"

func TestBasic(t *testing.T) {

	err := client.Remove(addr, dbname, 10)
	if err != nil && err != leveldb.NoDatabaseFound {
		log.Fatal(err)
	}

	db, err := client.Open(addr, dbname, true, 10)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal(err)
	}

	val, err := db.Get([]byte("mykey"))
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "myvalue" {
		t.Fatal("wrong value returned", string(val))
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func TestLookup(t *testing.T) {

	err := client.Remove(addr, dbname, 10)
	if err != nil && err != leveldb.NoDatabaseFound {
		log.Fatal(err)
	}

	db, err := client.Open(addr, dbname, true, 10)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal(err)
	}

	val, err := db.Get([]byte("mykey"))
	if err != nil {
		t.Fatal(err)
	}

	itr, err := db.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	key, val, err := itr.Next()
	if err != nil {
		t.Fatal(err)
	}

	if string(key) != "mykey" {
		t.Fatal("wrong key returned", string(val))
	}
	if string(val) != "myvalue" {
		t.Fatal("wrong value returned", string(val))
	}

	_, _, err = itr.Next()
	if err == nil {
		t.Fatal("should of been end of iterator")
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
