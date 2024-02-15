package zipappend

import (
	"encoding/binary"
)

const (
	fileHeaderSignature      = 0x04034b50
	directoryHeaderSignature = 0x02014b50
	directoryEndSignature    = 0x06054b50
	directory64LocSignature  = 0x07064b50
	directory64EndSignature  = 0x06064b50
	dataDescriptorSignature  = 0x08074b50 // de-facto standard; required by OS X Finder
	fileHeaderLen            = 30         // + filename + extra
	directoryHeaderLen       = 46         // + filename + extra + comment
	directoryEndLen          = 22         // + comment
	dataDescriptorLen        = 16         // four uint32: descriptor signature, crc32, compressed size, size
	dataDescriptor64Len      = 24         // two uint32: signature, crc32 | two uint64: compressed size, size
	directory64LocLen        = 20         //
	directory64EndLen        = 56         // + extra

	// Version numbers.
	zipVersion20 = 20 // 2.0
	zipVersion45 = 45 // 4.5 (reads and writes zip64 archives)
)

type dirEndRecord []byte

func (r *dirEndRecord) Offset() uint {
	return uint(binary.LittleEndian.Uint32((*r)[0x10:0x14]))
}

func (r *dirEndRecord) Size() int {
	return int(binary.LittleEndian.Uint32((*r)[0x0c:0x10]))
}

func (r *dirEndRecord) Records() int {
	return int(binary.LittleEndian.Uint16((*r)[0x0a:0x0c]))
}

type dirHeader []byte

func (h *dirHeader) NameLen() int {
	return int(binary.LittleEndian.Uint16((*h)[0x1c:0x1e]))

}

func (h *dirHeader) Name() string {
	return string((*h)[0x2e : 0x2e+h.NameLen()])
}

func binarySearch(name string, buf []byte, records, recSize int) int {
	s := 0
	e := records - 1
	for s <= e {
		m := (s + e) >> 1
		offset := m * recSize
		header := dirHeader(buf[offset : offset+recSize])

		if name < header.Name() {
			e = m - 1
		} else if name > header.Name() {
			s = m + 1
		} else {
			return m
		}
	}
	return -1
}

func FindKeys(zipInfo []byte, keys []string) {

}
