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
	DirectoryEndLen          = 22         // + comment
	dataDescriptorLen        = 16         // four uint32: descriptor signature, crc32, compressed size, size
	dataDescriptor64Len      = 24         // two uint32: signature, crc32 | two uint64: compressed size, size
	directory64LocLen        = 20         //
	directory64EndLen        = 56         // + extra

	// Version numbers.
	zipVersion20 = 20 // 2.0
	zipVersion45 = 45 // 4.5 (reads and writes zip64 archives)
)

type DirEndRecord []byte

func (r *DirEndRecord) Offset() uint {
	return uint(binary.LittleEndian.Uint32((*r)[0x10:0x14]))
}

func (r *DirEndRecord) SetOffset(offset uint) {
	binary.LittleEndian.PutUint32(((*r)[0x10:0x14]), uint32(offset))
}

func (r *DirEndRecord) Size() int {
	return int(binary.LittleEndian.Uint32((*r)[0x0c:0x10]))
}

func (r *DirEndRecord) SetSize(size int) {
	binary.LittleEndian.PutUint32(((*r)[0x0c:0x10]), uint32(size))
}

func (r *DirEndRecord) Records() int {
	return int(binary.LittleEndian.Uint16((*r)[0x0a:0x0c]))
}

func (r *DirEndRecord) SetRecords(records int) {
	binary.LittleEndian.PutUint16(((*r)[0x08:0x0a]), uint16(records))
	binary.LittleEndian.PutUint16(((*r)[0x0a:0x0c]), uint16(records))
}

type dirHeader []byte

func (h *dirHeader) NameLen() int {
	return int(binary.LittleEndian.Uint16((*h)[0x1c:0x1e]))
}

func (h *dirHeader) ExtraLen() int {
	return int(binary.LittleEndian.Uint16((*h)[0x1e:0x20]))
}

func (h *dirHeader) CommentLen() int {
	return int(binary.LittleEndian.Uint16((*h)[0x20:0x22]))
}

func (h *dirHeader) Name() string {
	return string((*h)[0x2e : 0x2e+h.NameLen()])
}

func (h *dirHeader) CompressedSize() int {
	return int(binary.LittleEndian.Uint32((*h)[0x14:0x18]))
}

func (h *dirHeader) Offset() uint {
	return uint(binary.LittleEndian.Uint32((*h)[0x2a:0x2e]))
}

func (h *dirHeader) SetOffset(offset uint) {
	binary.LittleEndian.PutUint32((*h)[0x2a:0x2e], uint32(offset))
}

func (h *dirHeader) Len() int {
	return directoryHeaderLen + h.NameLen() + h.ExtraLen() + h.CommentLen()
}

func binarySearch(name string, dirHeaders []byte, records, recSize int) int {
	s := 0
	e := records - 1
	for s <= e {
		m := (s + e) >> 1
		offset := m * recSize
		header := dirHeader(dirHeaders[offset : offset+recSize])

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

type FoundKey struct {
	Name           string
	Offset         uint
	CompressedSize int
}

func FindKeys(keys []string, dirHeaders []byte, records, recSize int) (fk []FoundKey) {

	fk = make([]FoundKey, 0, len(keys))
	for _, key := range keys {
		found := binarySearch(key, dirHeaders, records, recSize)
		if found != -1 {
			offset := found * recSize
			header := dirHeader(dirHeaders[offset : offset+recSize])
			fk = append(fk, FoundKey{
				Name:           header.Name(),
				Offset:         header.Offset(),
				CompressedSize: header.CompressedSize(),
			})
		}
	}
	return
}

// Append appends the directory of appendCD to baseCD
func Append(baseCD []byte, appendCD []byte, shift uint) (mergedCD []byte) {

	mergedCD = make([]byte, 0, len(baseCD)+len(appendCD))
	mergedCD = append(mergedCD, baseCD...)
	mergedCD = append(mergedCD, appendCD...)

	// Update offsets of directory that is appended
	for ptr := len(baseCD); ptr < len(mergedCD); {
		dh := dirHeader(mergedCD[ptr : ptr+directoryHeaderLen])
		dh.SetOffset(dh.Offset() + uint(shift))
		ptr += dh.Len()
	}

	return
}
