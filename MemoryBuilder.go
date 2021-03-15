package cdb

import (
	"bytes"
)

type CdbMemoryBuilder struct {
	w  *bytes.Buffer
	hp *builderImpl
}

func NewMemoryBuilder() (m *CdbMemoryBuilder) {
	m = &CdbMemoryBuilder{
		w: bytes.NewBuffer(make([]byte, headerSize)),
	}
	m.hp = newBuilderImpl(m.w)
	return m
}

func (m *CdbMemoryBuilder) PutKeyData(key []byte, data []byte) (err error) {
	return m.hp.putKeyData(m.w, key, data)
}

func (m *CdbMemoryBuilder) Close() (err error) {
	// Write hashValue tables and header.
	header, err := m.hp.writeHashValueTables(m.w)
	if err != nil {
		return err
	}
	copy(m.w.Bytes(), header)
	return nil
}

func (this *CdbMemoryBuilder) GetCdb() *Cdb {
	return New(bytes.NewReader(this.w.Bytes()))
}

func (this *CdbMemoryBuilder) GetBytes() []byte {
	return this.w.Bytes()
}
