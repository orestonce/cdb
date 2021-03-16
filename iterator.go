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
	keyLen      uint32
	keyHash     uint32
	loop        uint32
	slotPos     uint32
}

type cdbSequenceIterator struct { // 从头读到尾
	maxPos uint32
	rpos   uint32
}

func (this *Cdb) NewIterator() (it *CdbIterator) {
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
		return key, value, err
	}
	var buf []byte
	for {
		if this.find.loop >= this.find.supperIndex.entryListLength {
			return nil, nil, ErrNoData
		}
		slotKeyHash, slotKeyPos, err := this.c.readTupleUint32(this.find.slotPos)
		if err != nil {
			return nil, nil, err
		}
		if slotKeyPos == 0 {
			return nil, nil, ErrNoData
		}
		if slotKeyHash != this.find.keyHash {
			this.nextLoop()
			continue
		}
		rklen, rvlen, err := this.c.readTupleUint32(slotKeyPos)
		if err != nil {
			return nil, nil, err
		}
		if rklen != this.find.keyLen {
			this.nextLoop()
			continue
		}
		keyValueLength := int(rklen + rvlen)
		if keyValueLength > cap(buf) {
			buf = make([]byte, keyValueLength)
		}
		buf = buf[:keyValueLength]
		err = this.c.read(buf, slotKeyPos+8)
		if err != nil {
			return nil, nil, err
		}
		if !bytes.Equal(this.find.key, buf[:rklen]) {
			this.nextLoop()
			continue
		}
		this.nextLoop()
		return this.find.key, buf[rklen:], nil
	}
}

func (this *CdbIterator) ReadNextKey() (key []byte, vdata *io.SectionReader, err error) {
	if this.seq != nil {
		key, _, vdata, err = this.seqReadNextImpl(true)
		return key, vdata, err
	}
	return nil, nil, errors.New("cdb: ReadNextKey() only support sequence iterator.")
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

func (this *CdbIterator) nextLoop() {
	this.find.loop++
	this.find.slotPos += 8
	if this.find.slotPos == this.find.supperIndex.entryListPos+(this.find.supperIndex.entryListLength*8) {
		this.find.slotPos = this.find.supperIndex.entryListPos
	}
}
