// Package cdbV2 reads and writes cdb ("constant database") files.
//
// See the original cdb specification and C implementation by D. J. Bernstein
// at http://cr.yp.to/cdb.html.
package cdb

import (
	"encoding/binary"
	"io"
	"os"
)

const (
	headerSize     = uint32(256 * 8)
	entryTableSize = 256
)

type Cdb struct {
	r         io.ReaderAt
	f         *os.File
	header    [entryTableSize]index
	recordNum int
}

type index struct {
	entryListPos    uint32
	entryListLength uint32
}

// OpenFile opens the named file read-only and returns a new Cdb object.  The file
// should exist and be a cdb-format database file.
func OpenFile(name string) (db *Cdb, err error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	c, err := OpenReaderAt(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	c.f = f
	return c, nil
}

// Close closes the cdb for any further reads.
func (this *Cdb) Close() (err error) {
	if this.f != nil {
		return this.f.Close()
	}
	return nil
}

// OpenReaderAt creates a new Cdb from the given ReaderAt, which should be a cdb format database.
// Example: bytes.Reader/strings.Reader/os.File
func OpenReaderAt(r io.ReaderAt) (db *Cdb, err error) {
	db = &Cdb{
		r: r,
	}
	buf := make([]byte, headerSize)
	err = db.read(buf, 0)
	if err != nil {
		return nil, err
	}
	for idx := uint32(0); idx < entryTableSize; idx++ {
		db.header[idx].entryListPos = getNum(buf[idx*8:])
		entryListLength := getNum(buf[idx*8+4:])
		db.header[idx].entryListLength = entryListLength
		db.recordNum += int(entryListLength / 2)
	}
	return db, nil
}

func (this *Cdb) GetValue(key []byte) (value []byte, err error) {
	_, value, err = this.BeginFindKey(key).ReadNextKeyValue()
	return value, err
}

func (this *Cdb) BeginFindKey(key []byte) (it *CdbIterator) {
	keyHash := checksum(key)
	return &CdbIterator{
		c: this,
		find: &cdbFindIterator{
			key:         key,
			keyHash:     keyHash,
			supperIndex: this.header[keyHash%entryTableSize],
			loop:        0,
		},
	}
}

func (this *Cdb) IsKeyExists(key []byte) (exists bool, err error) {
	_, _, err = this.BeginFindKey(key).ReadNextKey()
	if err == ErrNoData {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (this *Cdb) read(buf []byte, pos uint32) error {
	_, err := this.r.ReadAt(buf, int64(pos))
	return err
}

func (this *Cdb) readTupleUint32(pos uint32) (n1 uint32, n2 uint32, err error) {
	var buf [8]byte
	err = this.read(buf[:8], pos)
	if err != nil {
		return 0, 0, err
	}
	return getNum(buf[:4]), getNum(buf[4:8]), nil
}

func (this *Cdb) GetRecordNum() (n int) {
	return this.recordNum
}

func getNum(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func checksum(data []byte) uint32 {
	var h = uint32(5381) // Initial cdb checksum value.
	for i := 0; i < len(data); i++ {
		h = ((h << 5) + h) ^ uint32(data[i])
	}
	return h
}
