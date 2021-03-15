package cdb

import (
	"bufio"
	"os"
)

type CdbFileBuilder struct {
	f  *os.File
	wb *bufio.Writer
	hp *builderImpl
}

func NewFileBuilder(name string) (m *CdbFileBuilder, err error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	if _, err = f.Seek(int64(headerSize), 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	m = &CdbFileBuilder{}
	m.f = f
	m.wb = bufio.NewWriter(f)
	m.hp = newBuilderImpl(m.wb)
	return m, nil
}

func (m *CdbFileBuilder) PutKeyData(key []byte, data []byte) (err error) {
	return m.hp.putKeyData(m.wb, key, data)
}

func (m *CdbFileBuilder) Close() (err error) {
	defer m.f.Close()

	// Write hashValue tables and header.
	// Create and reuse a single hashValue table.
	header, err := m.hp.writeHashValueTables(m.wb)
	if err != nil {
		return err
	}
	if err = m.wb.Flush(); err != nil {
		return err
	}

	if _, err = m.f.Seek(0, 0); err != nil {
		return err
	}

	_, err = m.f.Write(header)
	if err != nil {
		return err
	}
	err1 := m.f.Sync()
	if err1 != nil {
		return err1
	}
	return m.f.Close()
}
