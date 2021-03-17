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

func TestNewMemoryWriter(t *testing.T) {
	writer := cdb.NewMemoryWriter()
	err := writer.WriteKeyValue([]byte("key"), []byte("value"))
	if err != nil {
		panic(err)
	}
	db, err := writer.Freeze()
	if err != nil {
		panic(err)
	}
	it := db.BeginFindKey([]byte("key"))
	_, value, err := it.ReadNextKeyValue()
	if err != nil {
		panic(err)
	}
	if string(value) != `value` {
		panic("value check failed : " + string(value))
	}
	_, _, err = it.ReadNextKeyValue()
	if err != cdb.ErrNoData {
		panic("expect ErrNoData")
	}
}

func TestNewMemoryWriter2(t *testing.T) {
	writer := cdb.NewMemoryWriter()
	err := writer.WriteKeyValue([]byte("ke"), []byte("value"))
	if err != nil {
		panic(err)
	}
	bn := writer.GetByteNum()
	if bn != 2048+(2+5)+1*24 {
		panic("unexpected ByteNum: " + strconv.Itoa(int(bn)))
	}
	data, err := writer.GetMemoryWriterBytes()
	if err != nil {
		panic(err)
	}
	if len(data) != int(bn) {
		panic("invalid data length: " + strconv.Itoa(len(data)) + ", " + strconv.Itoa(int(bn)))
	}
	bn2 := writer.GetByteNum()
	if bn != bn2 {
		panic("invalid bn2: " + strconv.Itoa(int(bn2)))
	}
}

func TestNewMemoryWriter3(t *testing.T) {
	writer := cdb.NewMemoryWriter()
	err := writer.WriteKeyValue([]byte("key"), []byte("v"))
	if err != nil {
		panic(err)
	}
	db, err := writer.Freeze()
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
	err = writer.Close()
	if err != nil {
		panic(err)
	}
}

func TestNewMemoryWriter4(t *testing.T) {
	builder := cdb.NewMemoryWriter()
	db, err := builder.Freeze()
	if err != nil {
		panic(err)
	}
	num := db.GetRecordNum()
	if num != 0 {
		panic("expected 0 num: " + strconv.Itoa(num))
	}
	it := db.BeginIterator()
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

func TestNewMemoryWriter5(t *testing.T) {
	memoryWriter := cdb.NewMemoryWriter()
	for key, valueList := range keyValueMap {
		for _, value := range valueList {
			err := memoryWriter.WriteKeyValue([]byte(key), []byte(value))
			if err != nil {
				panic(err)
			}
		}
	}
	db, err := memoryWriter.Freeze()
	if err != nil {
		panic(err)
	}
	assertDbEqual(db)
	err = memoryWriter.Close()
	if err != nil {
		panic(err)
	}
	const testFileName = "testdata/TestNewMemoryWriter5.cdb"
	data, err := memoryWriter.GetMemoryWriterBytes()
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

func TestNewFileWriter(t *testing.T) {
	const testFileName = "testdata/TestNewFileWriter.cdb"
	fileWriter, err := cdb.NewFileWriter(testFileName)
	if err != nil {
		panic(err)
	}
	for key, valueList := range keyValueMap {
		for _, value := range valueList {
			err = fileWriter.WriteKeyValue([]byte(key), []byte(value))
			if err != nil {
				panic(err)
			}
		}
	}
	err = fileWriter.Close()
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
		it := db.BeginFindKey([]byte(key))
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
	it := db.BeginIterator()
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
