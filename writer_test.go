package cdb_test

import (
	"github.com/orestonce/cdb"
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
