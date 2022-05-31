package cdb

import (
	"strconv"
	"testing"
)

func BenchmarkCdb_GetValue(b *testing.B) {
	b.StopTimer()
	w := NewMemoryWriter()
	for i := 0; i < b.N; i++ {
		err := w.WriteKeyValue([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		if err != nil {
			panic(err)
		}
	}
	db, err := w.Freeze()
	if err != nil {
		panic(err)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		vb, err := db.GetValue([]byte(key))
		if err != nil {
			panic(err)
		}
		if string(vb) != key {
			panic(string(vb))
		}
	}
}

func BenchmarkCdb_GetValue2(b *testing.B) {
	b.StopTimer()
	db := map[string]string{}
	for i := 0; i < b.N; i++ {
		db[strconv.Itoa(i)] = strconv.Itoa(i)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		value, ok := db[key]
		if !ok {
			panic(key)
		}
		if value != key {
			panic(value)
		}
	}
}
