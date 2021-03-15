package cdb_test

import (
	"bytes"
	"github.com/orestonce/cdb"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func TestNewMemoryBuilder(t *testing.T) {
	builder := cdb.NewMemoryBuilder()
	err := builder.Close()
	if err != nil {
		panic(err)
	}
	db := builder.GetCdb()
	num, err := db.GetRecordNum()
	if err != nil {
		panic(err)
	}
	if num != 0 {
		panic("expected 0 num: " + strconv.Itoa(num))
	}
	if len(builder.GetBytes()) != 2048 {
		panic("expect 2048, " + strconv.Itoa(len(builder.GetBytes())))
	}
	it, err := db.NewIterator()
	if err != nil {
		panic(err)
	}
	if it.HasNext() {
		panic("expect !it.HasNext()")
	}
}

var keyValueMap = map[string][]string{
	"":     {""},
	"k1":   {"value1", "value2"},
	"user": {"password", "password9"},
	"data": {"origin", "data", "run", "origin", "data2"},
}

func TestNewMemoryBuilder2(t *testing.T) {
	builder := cdb.NewMemoryBuilder()
	for key, valueList := range keyValueMap {
		for _, value := range valueList {
			err := builder.PutKeyData([]byte(key), []byte(value))
			if err != nil {
				panic(err)
			}
		}
	}
	err := builder.Close()
	if err != nil {
		panic(err)
	}
	db := builder.GetCdb()
	assertDbEqual(db)
	const testFileName = "testdata/TestNewMemoryBuilder2.cdb"
	err = ioutil.WriteFile(testFileName, builder.GetBytes(), 0777)
	if err != nil {
		panic(err)
	}
	db, err = cdb.Open(testFileName)
	if err != nil {
		panic(err)
	}
	dbValue1, err := db.Data([]byte("k1"))
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(dbValue1, []byte("value1")) {
		panic("expect value1 " + string(dbValue1))
	}
	err = db.Close()
	if err != nil {
		panic(err)
	}
	err = os.Remove(testFileName)
	if err != nil {
		panic(err)
	}
}

func TestNewFileBuilder(t *testing.T) {
	const testFileName = "testdata/TestNewFileBuilder.cdb"
	builder, err := cdb.NewFileBuilder(testFileName)
	if err != nil {
		panic(err)
	}
	for key, valueList := range keyValueMap {
		for _, value := range valueList {
			err = builder.PutKeyData([]byte(key), []byte(value))
			if err != nil {
				panic(err)
			}
		}
	}
	err = builder.Close()
	if err != nil {
		panic(err)
	}
	db, err := cdb.Open(testFileName)
	if err != nil {
		panic(err)
	}
	assertDbEqual(db)
	err = db.Close()
	if err != nil {
		panic(err)
	}
	err = os.Remove(testFileName)
	if err != nil {
		panic(err)
	}
}

func assertDbEqual(db *cdb.Cdb) {
	for key, valueList := range keyValueMap {
		db.FindStart()
		for idx, value := range valueList {
			dbValue, err := db.FindNextData([]byte(key))
			if err != nil {
				panic(err)
			}
			if !bytes.Equal(dbValue, []byte(value)) {
				panic("value not equal " + key + " " + strconv.Itoa(idx))
			}
		}
	}
	it, err := db.NewIterator()
	if err != nil {
		panic(err)
	}
	for it.HasNext() {
		dbKey, dbValue, err := it.ReadNextKeyValue()
		if err != nil {
			panic(err)
		}
		valueList, ok := keyValueMap[string(dbKey)]
		if !ok {
			panic("memory not exists " + string(dbKey))
		}
		existsValue := false
		for _, v := range valueList {
			if v == string(dbValue) {
				existsValue = true
				break
			}
		}
		if !existsValue {
			panic("memory not exists2 " + string(dbKey))
		}
	}
}

func TestOpen2(t *testing.T) {
	db, err := cdb.Open("testdata/test.cdb")
	if err != nil {
		panic(err)
	}
	it, err := db.NewIterator()
	if err != nil {
		panic(err)
	}
	n, err := db.GetRecordNum()
	if err != nil {
		panic(err)
	}
	if n != 9 {
		panic("expect n == 9 : " + strconv.Itoa(n))
	}
	for it.HasNext() {
		_, _, err := it.ReadNextKeyValue()
		if err != nil {
			panic(err)
		}
	}
	err = db.Close()
	if err != nil {
		panic(err)
	}
}

func TestOpen(t *testing.T) {
	db, err := cdb.Open("testdata/random.cdb")
	if err != nil {
		panic(err)
	}
	it, err := db.NewIterator()
	if err != nil {
		panic(err)
	}
	n, err := db.GetRecordNum()
	if err != nil {
		panic(err)
	}
	if n != 100 {
		panic("expect n == 100 " + strconv.Itoa(n))
	}
	var cnt int
	for it.HasNext() {
		_, _, err := it.ReadNextKeyValue()
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
