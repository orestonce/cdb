package cdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strconv"
)

type CdbWriter struct {
	f         *os.File      // 1
	b         *bytes.Buffer // 2
	htables   [entryTableSize][]slot
	pos       uint32
	isFreezed bool
}

func NewFileWriter(name string) (m *CdbWriter, err error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	if _, err = f.Seek(int64(headerSize), 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &CdbWriter{
		f:       f,
		b:       nil,
		htables: [256][]slot{},
		pos:     headerSize,
	}, nil
}

func NewMemoryWriter() (m *CdbWriter) {
	return &CdbWriter{
		f:       nil,
		b:       bytes.NewBuffer(make([]byte, headerSize)),
		htables: [256][]slot{},
		pos:     headerSize,
	}
}

func (this *CdbWriter) WriteKeyValue(key []byte, data []byte) (err error) {
	if this.isFreezed {
		return this.invalidFreezeState()
	}
	klen, dlen := uint32(len(key)), uint32(len(data))
	w := this.getWriter()
	err = writeNums(w, klen, dlen)
	if err != nil {
		return err
	}
	_, err = w.Write(key)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	h := checksum(key)
	tableNum := h % entryTableSize
	this.htables[tableNum] = append(this.htables[tableNum], slot{h, this.pos})
	this.pos += 8 + klen + dlen
	return nil
}

func (this *CdbWriter) getWriter() io.Writer {
	if this.f != nil {
		return this.f
	}
	return this.b
}

func (this *CdbWriter) Freeze() (db *Cdb, err error) {
	if this.isFreezed {
		return nil, this.invalidFreezeState()
	}
	this.isFreezed = true
	maxSlots := 0
	for _, slots := range this.htables {
		if len(slots) > maxSlots {
			maxSlots = len(slots)
		}
	}
	slotTable := make([]slot, maxSlots*2)

	header := make([]byte, headerSize)
	// Write hashValue tables.
	for i, slots := range this.htables {
		if slots == nil {
			putNum(header[i*8:], this.pos)
			continue
		}

		nslots := uint32(len(slots) * 2)
		hashSlotTable := slotTable[:nslots]
		// Reset table slots.
		for j := 0; j < len(hashSlotTable); j++ {
			hashSlotTable[j].h = 0
			hashSlotTable[j].pos = 0
		}

		for _, slot := range slots {
			slotPos := (slot.h / entryTableSize) % nslots
			for hashSlotTable[slotPos].pos != 0 {
				slotPos++
				if slotPos == uint32(len(hashSlotTable)) {
					slotPos = 0
				}
			}
			hashSlotTable[slotPos] = slot
		}
		if err = writeSlots(this.getWriter(), hashSlotTable); err != nil {
			return nil, err
		}
		putNum(header[i*8:], this.pos)
		putNum(header[i*8+4:], nslots)
		this.pos += 8 * nslots
	}
	if this.f != nil {
		_, err = this.f.WriteAt(header, 0)
		if err != nil {
			return nil, err
		}
		err = this.f.Sync()
		if err != nil {
			return nil, err
		}
		return OpenReaderAt(this.f)
	}
	copy(this.b.Bytes(), header)
	return OpenReaderAt(bytes.NewReader(this.b.Bytes()))
}

func (this *CdbWriter) Close() (err error) {
	if !this.isFreezed {
		_, err = this.Freeze()
		if err != nil {
			if this.f != nil {
				_ = this.f.Close()
			}
			return err
		}
	}
	if this.f != nil {
		return this.f.Close()
	}
	return nil
}

func (this *CdbWriter) GetBytes() (bs []byte, err error) {
	if !this.isFreezed {
		return nil, this.invalidFreezeState()
	}
	if this.b == nil {
		return nil, errors.New("cdb: GetBytes() only support memory writer.")
	}
	return this.b.Bytes(), nil
}

func putNum(buf []byte, x uint32) {
	binary.LittleEndian.PutUint32(buf, x)
}

func writeNums(w io.Writer, x, y uint32) (err error) {
	var buf [8]byte
	putNum(buf[:], x)
	putNum(buf[4:], y)
	_, err = w.Write(buf[:])
	return err
}

type slot struct {
	h, pos uint32
}

func writeSlots(w io.Writer, slots []slot) (err error) {
	var buf [8]byte
	for _, np := range slots {
		putNum(buf[:], np.h)
		putNum(buf[4:], np.pos)
		if _, err = w.Write(buf[:8]); err != nil {
			return err
		}
	}
	return nil
}

func (this *CdbWriter) invalidFreezeState() error {
	return errors.New("cdb: invalid freeze state: " + strconv.FormatBool(this.isFreezed))
}
