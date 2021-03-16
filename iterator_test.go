package cdb_test

import (
	"fmt"
	"github.com/orestonce/cdb"
	"io"
	"io/ioutil"
	"strconv"
	"testing"
)

func TestCdb_NewIterator(t *testing.T) {
	// 顺序迭代
	const maxIndex = 9
	writer := cdb.NewMemoryWriter()
	for idx := 0; idx < maxIndex; idx++ {
		err := writer.WriteKeyValue([]byte(strconv.Itoa(idx)), []byte(strconv.Itoa(idx+1)))
		if err != nil {
			panic(err)
		}
	}
	db, err := writer.Freeze()
	if err != nil {
		panic(err)
	}
	it := db.BeginIterator()
	for idx := 0; idx < maxIndex; idx++ {
		var key, value []byte
		if idx%2 == 0 {
			key, value, err = it.ReadNextKeyValue()
		} else {
			var vdata *io.SectionReader
			key, vdata, err = it.ReadNextKey()
			if err != nil {
				panic(err)
			}
			value, err = ioutil.ReadAll(vdata)
		}
		if err != nil {
			panic(err)
		}
		if string(key) != strconv.Itoa(idx) || string(value) != strconv.Itoa(idx+1) {
			panic("key value not match.")
		}
	}
	_, _, err = it.ReadNextKeyValue()
	if err != cdb.ErrNoData {
		panic(fmt.Sprint("unexpect error value", err))
	}
	_ = db.Close()
	content, err := writer.GetBytes()
	if err != nil {
		panic(err)
	}
	if len(content) != 2282 {
		panic("invalid content length: " + strconv.Itoa(len(content)))
	}
	exists, err := db.IsKeyExists([]byte("5"))
	if err != nil {
		panic(err)
	}
	if !exists {
		panic("key not exists: 5")
	}
	exists, err = db.IsKeyExists([]byte("60"))
	if err != nil {
		panic(err)
	}
	if exists {
		panic("expect key not exists, but it's exists.")
	}
}
