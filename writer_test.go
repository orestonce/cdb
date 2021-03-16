package cdb_test

import (
	"testing"
	"github.com/orestonce/cdb"
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
	db.BeginFindKey()
}
