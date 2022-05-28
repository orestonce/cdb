package cdb

import (
	"bytes"
	"os"
	"sort"
)

func MapStringToBytes(m map[string]string) (data []byte) {
	w := NewMemoryWriter()
	for _, key := range mapStringGetSortedKeyList(m) {
		err := w.WriteKeyValue([]byte(key), []byte(m[key]))
		if err != nil {
			panic(err)
		}
	}
	data, err := w.GetMemoryWriterBytes()
	if err != nil {
		panic(err)
	}
	return data
}

func mapStringGetSortedKeyList(m map[string]string) (keyList []string) {
	for key := range m {
		keyList = append(keyList, key)
	}
	sort.Strings(keyList)
	return keyList
}

func BytesGetValueString(data []byte, key string) (value string, err error) {
	db, err := OpenReaderAt(bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	it := db.BeginFindKey([]byte(key))
	_, vb, err := it.ReadNextKeyValue()
	if err != nil {
		return "", err
	}
	return string(vb), nil
}

func FileGetValueString(fPath string, key string) (value string, err error) {
	db, err := OpenFile(fPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrNoData
		}
		return "", err
	}
	defer db.Close()
	it := db.BeginFindKey([]byte(key))
	_, vb, err := it.ReadNextKeyValue()
	if err != nil {
		return "", err
	}
	return string(vb), nil
}

func FileRewriteKeyValue(fPath string, key string, value string) (err error) {
	return FileRewriteMapString(fPath, map[string]string{
		key: value,
	})
}

func FileRewriteMapString(fPath string, m map[string]string) (err error) {
	if len(m) == 0 {
		return nil
	}
	from, err := OpenFile(fPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	closeFrom := func() {
		if from != nil {
			from.Close()
		}
	}
	to, err := NewFileWriter(fPath + ".tmp")
	if err != nil {
		closeFrom()
		return err
	}
	closeTo := func() {
		to.Close()
		os.Remove(fPath + ".tmp")
	}
	if from != nil {
		it := from.BeginIterator()
		for {
			var kb, vb []byte
			kb, vb, err = it.ReadNextKeyValue()
			if err != nil {
				if err == ErrNoData {
					break
				}
				closeFrom()
				closeTo()
				return err
			}
			if _, ok := m[string(kb)]; ok {
				continue
			}
			err = to.WriteKeyValue(kb, vb)
			if err != nil {
				closeFrom()
				closeTo()
				return err
			}
		}
		err = from.Close()
		if err != nil {
			closeTo()
			return err
		}
	}
	for _, key := range mapStringGetSortedKeyList(m) {
		value := m[key]
		err = to.WriteKeyValue([]byte(key), []byte(value))
		if err != nil {
			closeTo()
			return err
		}
	}
	err = to.Close()
	if err != nil {
		closeTo()
		return err
	}
	return os.Rename(fPath+".tmp", fPath)
}
