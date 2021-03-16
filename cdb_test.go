package cdb_test

import (
	"bytes"
	"fmt"
	"github.com/orestonce/cdb"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func TestNewMemoryBuilder3(t *testing.T) {
	builder := cdb.NewMemoryWriter()
	err := builder.WriteKeyValue([]byte("key"), []byte("v"))
	if err != nil {
		panic(err)
	}
	db, err := builder.Freeze()
	if err != nil {
		panic(err)
	}
	value, err := db.GetValue([]byte("key"))
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(value, []byte("v")) {
		panic("value not equal " + string(value))
	}
	err = builder.Close()
	if err != nil {
		panic(err)
	}
}

func TestNewMemoryBuilder(t *testing.T) {
	builder := cdb.NewMemoryWriter()
	db, err := builder.Freeze()
	if err != nil {
		panic(err)
	}
	num := db.GetRecordNum()
	if num != 0 {
		panic("expected 0 num: " + strconv.Itoa(num))
	}
	it := db.NewIterator()
	if _, _, err = it.ReadNextKeyValue(); err != cdb.ErrNoData {
		panic("expect !it.HasNext()")
	}
	err = builder.Close()
	if err != nil {
		panic(err)
	}
}

var keyValueMap = map[string][]string{
	"":     {""},
	"k1":   {"value1", "value2"},
	"user": {"password", "password9"},
	"data": {"origin", "data", "run", "origin", "data2"},
}

func TestNewMemoryBuilder2(t *testing.T) {
	builder := cdb.NewMemoryWriter()
	for key, valueList := range keyValueMap {
		for _, value := range valueList {
			err := builder.WriteKeyValue([]byte(key), []byte(value))
			if err != nil {
				panic(err)
			}
		}
	}
	db, err := builder.Freeze()
	if err != nil {
		panic(err)
	}
	assertDbEqual(db)
	err = builder.Close()
	if err != nil {
		panic(err)
	}
	const testFileName = "testdata/TestNewMemoryBuilder2.cdb"
	data, err := builder.GetBytes()
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(testFileName, data, 0777)
	if err != nil {
		panic(err)
	}
	db, err = cdb.OpenFile(testFileName)
	if err != nil {
		panic(err)
	}
	dbValue1, err := db.GetValue([]byte("k1"))
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
	builder, err := cdb.NewFileWriter(testFileName)
	if err != nil {
		panic(err)
	}
	for key, valueList := range keyValueMap {
		for _, value := range valueList {
			err = builder.WriteKeyValue([]byte(key), []byte(value))
			if err != nil {
				panic(err)
			}
		}
	}
	err = builder.Close()
	if err != nil {
		panic(err)
	}
	db, err := cdb.OpenFile(testFileName)
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
		it := db.FindKey([]byte(key))
		for idx, value := range valueList {
			_, dbValue, err := it.ReadNextKeyValue()
			if err != nil {
				panic(err)
			}
			if !bytes.Equal(dbValue, []byte(value)) {
				panic("value not equal " + key + " " + strconv.Itoa(idx))
			}
		}
		_, _, err := it.ReadNextKeyValue()
		if err != cdb.ErrNoData {
			panic(fmt.Sprint("unexpected error ", err))
		}
	}
	it := db.NewIterator()
	for {
		dbKey, dbValue, err := it.ReadNextKeyValue()
		if err == cdb.ErrNoData {
			break
		}
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
	db, err := cdb.OpenFile("testdata/test.cdb")
	if err != nil {
		panic(err)
	}
	it := db.NewIterator()
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

func TestOpen(t *testing.T) {
	db, err := cdb.OpenFile("testdata/random.cdb")
	if err != nil {
		panic(err)
	}
	n := db.GetRecordNum()
	if n != 100 {
		panic("expect n == 100 " + strconv.Itoa(n))
	}
	var cnt int
	it := db.NewIterator()
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
