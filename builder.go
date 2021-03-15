package cdb

import (
	"encoding/binary"
	"io"
)

type builderImpl struct {
	buf     []byte
	hw      io.Writer
	htables [tableSize][]slot
	pos     uint32
}

func newBuilderImpl(w io.Writer) (impl *builderImpl) {
	impl = &builderImpl{
		buf: make([]byte, 8),
		pos: headerSize,
		hw:  w,
	}
	return impl
}

func (hp *builderImpl) putKeyData(w io.Writer, key []byte, data []byte) (err error) {
	klen, dlen := uint32(len(key)), uint32(len(data))
	err = writeNums(w, klen, dlen, hp.buf)
	if err != nil {
		return err
	}
	_, err = hp.hw.Write(key)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	h := checksum(key)
	tableNum := h % tableSize
	hp.htables[tableNum] = append(hp.htables[tableNum], slot{h, hp.pos})
	hp.pos += 8 + klen + dlen
	return nil
}

func (hp *builderImpl) writeHashValueTables(w io.Writer) (header []byte, err error) {
	maxSlots := 0
	for _, slots := range hp.htables {
		if len(slots) > maxSlots {
			maxSlots = len(slots)
		}
	}
	slotTable := make([]slot, maxSlots*2)

	header = make([]byte, headerSize)
	// Write hashValue tables.
	for i, slots := range hp.htables {
		if slots == nil {
			putNum(header[i*8:], hp.pos)
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
			slotPos := (slot.h / tableSize) % nslots
			for hashSlotTable[slotPos].pos != 0 {
				slotPos++
				if slotPos == uint32(len(hashSlotTable)) {
					slotPos = 0
				}
			}
			hashSlotTable[slotPos] = slot
		}

		if err = writeSlots(w, hashSlotTable, hp.buf); err != nil {
			return nil, err
		}

		putNum(header[i*8:], hp.pos)
		putNum(header[i*8+4:], nslots)
		hp.pos += 8 * nslots
	}
	return header, nil
}

func putNum(buf []byte, x uint32) {
	binary.LittleEndian.PutUint32(buf, x)
}

func writeNums(w io.Writer, x, y uint32, buf []byte) (err error) {
	putNum(buf, x)
	putNum(buf[4:], y)
	_, err = w.Write(buf[:8])
	return err
}

type slot struct {
	h, pos uint32
}

func writeSlots(w io.Writer, slots []slot, buf []byte) (err error) {
	for _, np := range slots {
		putNum(buf, np.h)
		putNum(buf[4:], np.pos)
		if _, err = w.Write(buf[:8]); err != nil {
			return err
		}
	}
	return nil
}
