package cdb_test

import (
	"github.com/orestonce/cdb"
	"strconv"
	"testing"
)

func TestOpenFile(t *testing.T) {
	db, err := cdb.OpenFile("testdata/test.cdb")
	if err != nil {
		panic(err)
	}
	it := db.BeginIterator()
	n := db.GetRecordNum()
	if n != 9 {
		panic("expect n == 9 : " + strconv.Itoa(n))
	}
	for {
		_, _, err := it.ReadNextKeyValue()
		if err == cdb.ErrNoData {
			break
		}
		if err != nil {
			panic(err)
		}
	}
	err = db.Close()
	if err != nil {
		panic(err)
	}
}

func TestOpenFile2(t *testing.T) {
	db, err := cdb.OpenFile("testdata/random.cdb")
	if err != nil {
		panic(err)
	}
	n := db.GetRecordNum()
	if n != 100 {
		panic("expect n == 100 " + strconv.Itoa(n))
	}
	var cnt int
	it := db.BeginIterator()
	for {
		_, _, err := it.ReadNextKeyValue()
		if err == cdb.ErrNoData {
			break
		}
		if err != nil {
			panic(err)
		}
		cnt++
	}
	if cnt != n {
		panic("expect cnt == 100 : " + strconv.Itoa(cnt))
	}
	err = db.Close()
	if err != nil {
		panic(err)
	}
}
