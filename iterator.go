package cdb

import (
	"bytes"
	"errors"
	"io"
)

var ErrNoData = errors.New("cdb: error no data")

type CdbIterator struct {
	c    *Cdb
	seq  *cdbSequenceIterator
	find *cdbFindIterator
}

type cdbFindIterator struct { // 查找某个key的value
	key         []byte
	supperIndex index
	keyHash     uint32
	loop        uint32
}

type cdbSequenceIterator struct { // 从头读到尾
	maxPos uint32
	rpos   uint32
}

func (this *Cdb) BeginIterator() (it *CdbIterator) {
	return &CdbIterator{
		c: this,
		seq: &cdbSequenceIterator{
			maxPos: this.header[0].entryListPos,
			rpos:   headerSize,
		},
	}
}

func (this *CdbIterator) ReadNextKeyValue() (key []byte, value []byte, err error) {
	if this.seq != nil {
		key, value, _, err = this.seqReadNextImpl(false)
	} else {
		key, value, _, err = this.findReadNextImpl(false)
	}
	return key, value, err
}

func (this *CdbIterator) ReadNextKey() (key []byte, vdata *io.SectionReader, err error) {
	if this.seq != nil {
		key, _, vdata, err = this.seqReadNextImpl(true)
	} else {
		key, _, vdata, err = this.findReadNextImpl(true)
	}
	return key, vdata, err
}

func (this *CdbIterator) seqReadNextImpl(onlyKey bool) (key []byte, value []byte, vdata *io.SectionReader, err error) {
	if this.seq.rpos >= this.seq.maxPos {
		return nil, nil, nil, ErrNoData
	}
	klen, vlen, err := this.c.readTupleUint32(this.seq.rpos)
	if err != nil {
		return nil, nil, nil, err
	}
	var buf []byte
	if onlyKey {
		buf = make([]byte, klen)
	} else {
		buf = make([]byte, klen+vlen)
	}
	err = this.c.read(buf, this.seq.rpos+8)
	if err != nil {
		return nil, nil, nil, err
	}
	key = buf[:klen]
	if onlyKey {
		vdata = io.NewSectionReader(this.c.r, int64(this.seq.rpos+8+klen), int64(vlen))
	} else {
		value = buf[klen:]
	}
	this.seq.rpos = this.seq.rpos + 8 + klen + vlen
	return key, value, vdata, err
}

func (this *CdbIterator) findReadNextImpl(onlyKey bool) (key []byte, value []byte, vdata *io.SectionReader, err error) {
	key = this.find.key
	var buf []byte
	for {
		if this.find.loop >= this.find.supperIndex.entryListLength {
			return key, nil, nil, ErrNoData
		}
		slotPos := this.find.supperIndex.entryListPos + (((this.find.keyHash/entryTableSize + this.find.loop) % this.find.supperIndex.entryListLength) * 8)
		slotKeyHash, slotKeyPos, err := this.c.readTupleUint32(slotPos)
		if err != nil {
			return key, nil, nil, err
		}
		if slotKeyPos == 0 {
			return key, nil, nil, ErrNoData
		}
		if slotKeyHash != this.find.keyHash {
			this.find.loop++
			continue
		}
		recordKeyLength, recordValueLength, err := this.c.readTupleUint32(slotKeyPos)
		if err != nil {
			return key, nil, nil, err
		}
		if recordKeyLength != uint32(len(key)) {
			this.find.loop++
			continue
		}
		var bufLength int
		if onlyKey {
			bufLength = int(recordKeyLength)
		} else {
			bufLength = int(recordKeyLength + recordValueLength)
		}
		if bufLength > cap(buf) {
			buf = make([]byte, bufLength)
		}
		buf = buf[:bufLength]
		err = this.c.read(buf, slotKeyPos+8)
		if err != nil {
			return key, nil, nil, err
		}
		this.find.loop++
		if !bytes.Equal(this.find.key, buf[:recordKeyLength]) {
			continue
		}
		if onlyKey {
			vdata = io.NewSectionReader(this.c.r, int64(slotKeyPos+8+recordKeyLength), int64(recordValueLength))
		} else {
			value = buf[recordKeyLength:]
		}
		return key, value, vdata, nil
	}
}
