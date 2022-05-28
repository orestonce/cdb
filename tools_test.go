package cdb

import (
	"os"
	"testing"
)

func TestMapStringToBytes(t *testing.T) {
	data := MapStringToBytes(map[string]string{
		"1": "2",
		"3": "4",
	})
	v, err := BytesGetValueString(data, "1")
	if err != nil {
		panic(err)
	}
	if v != "2" {
		panic(v)
	}
	v, err = BytesGetValueString(data, "3")
	if err != nil {
		panic(err)
	}
	if v != "4" {
		panic(v)
	}
	_, err = BytesGetValueString(data, "5")
	if err != ErrNoData {
		panic(err)
	}
}

func TestFileGetValueString(t *testing.T) {
	_, err := FileGetValueString("testdata/no_file.cdb", "key")
	if err != ErrNoData {
		panic(err)
	}
	data := MapStringToBytes(map[string]string{
		"key": "value",
	})
	const fName = "testdata/tmp1.cdb"
	err = os.WriteFile(fName, data, 0777)
	if err != nil {
		panic(err)
	}
	value, err := FileGetValueString(fName, "key")
	if err != nil {
		panic(err)
	}
	if value != "value" {
		panic(err)
	}
	_, err = FileGetValueString(fName, "no_key")
	if err != ErrNoData {
		panic(err)
	}
	err = FileRewriteKeyValue(fName, "no_key", "v1")
	if err != nil {
		panic(err)
	}
	value, err = FileGetValueString(fName, "key")
	if err != nil {
		panic(err)
	}
	if value != "value" {
		panic(err)
	}
	value, err = FileGetValueString(fName, "no_key")
	if err != nil {
		panic(err)
	}
	if value != "v1" {
		panic(err)
	}
	err = os.Remove(fName)
	if err != nil {
		panic(err)
	}
}
