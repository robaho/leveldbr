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

func TestSnapshotLookup(t *testing.T) {

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

	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("mykey2"), []byte("myvalue2"))
	if err != nil {
		t.Fatal(err)
	}

	val, err := snapshot.Get([]byte("mykey"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "myvalue" {
		t.Fatal("wrong value returned", string(val))
	}

	_, err = snapshot.Get([]byte("mykey2"))
	if err != leveldb.KeyNotFound {
		t.Fatal("mykey2 should not have been found", err)
	}

	itr, err := snapshot.Lookup(nil, nil)
	count := 0
	for {
		_, _, err = itr.Next()
		if err == leveldb.EndOfIterator {
			break
		}
		count++
	}
	if count != 1 {
		t.Fatal("wrong count", count)
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
