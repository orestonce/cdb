package cdb

import (
	"io"
	"io/ioutil"
)

type CdbIterator struct {
	c       *Cdb
	maxNPos uint32
	rpos    uint32
}

func (c *Cdb) NewIterator() (it *CdbIterator, err error) {
	buf := c.buf[:4]
	err = c.read(buf, 0)
	if err != nil {
		return nil, err
	}
	it = &CdbIterator{
		c:       c,
		maxNPos: getNum(buf),
		rpos:    headerSize,
	}
	return it, nil
}

func (this *CdbIterator) HasNext() bool {
	return this.rpos < this.maxNPos
}

func (this *CdbIterator) ReadNextKey() (key []byte, rdata *io.SectionReader, err error) {
	klen, vlen, err := this.c.read2Num(this.rpos)
	if err != nil {
		return nil, nil, err
	}
	key = make([]byte, klen)
	err = this.c.read(key, this.rpos+8)
	if err != nil {
		return nil, nil, err
	}
	rdata = io.NewSectionReader(this.c.r, int64(this.rpos+8+klen), int64(vlen))
	this.rpos = this.rpos + 8 + klen + vlen
	return key, rdata, nil
}

func (this *CdbIterator) ReadNextKeyValue() (key []byte, value []byte, err error) {
	key, rdata, err := this.ReadNextKey()
	if err != nil {
		return nil, nil, err
	}
	value, err = ioutil.ReadAll(rdata)
	return key, value, err
}
