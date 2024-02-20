package zipappend

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func loadZip(tb testing.TB, fname string) (dirHeaders []byte, records, recSize int) {
	file, err := os.Open(fname)
	if err != nil {
		tb.Fatal(err)
	}
	defer file.Close()

	var stat os.FileInfo
	if stat, err = file.Stat(); err != nil {
		tb.Fatal(err)
	} else if stat.Size() < DirectoryEndLen {
		tb.Fatalf("file too small: %d", stat.Size())
	}
	start := stat.Size() - DirectoryEndLen
	bufEocd := [DirectoryEndLen]byte{}
	if _, err = file.ReadAt(bufEocd[:], start); err != nil {
		tb.Fatal(err)
	}

	eocd := DirEndRecord(bufEocd[:])

	offset := eocd.Offset()
	size := eocd.Size()
	records = eocd.Records()

	bufCentDir := make([]byte, uint(stat.Size())-offset)
	var n int
	if n, err = file.ReadAt(bufCentDir, int64(offset)); err != nil {
		tb.Fatal(err)
	} else if n < size {
		tb.Fatalf("could not read central directory: %d", n)
	}

	// eodl64 := buf[len(buf)-directory64LocLen-directoryEndLen : len(buf)-directoryEndLen]
	// fmt.Println(hex.Dump(eodl64))

	// eocd64 := buf[len(buf)-directory64EndLen-directory64LocLen-directoryEndLen : len(buf)-directory64LocLen-directoryEndLen]
	// fmt.Println(hex.Dump(eocd64))

	// fmt.Printf(" offset = %08x\n", offset)
	// fmt.Printf("   size = %08x\n", size)
	// fmt.Printf("records = %0d\n", records)

	dirHeaders = bufCentDir[:uint(size)]
	recSize = int(size / records)

	if false {
		dumpDirHeaders(dirHeaders, recSize, records)
	}

	return
}

func TestBinarySearch(t *testing.T) {
	dirHeaders, records, recSize := loadZip(t, "testdata/"+"sorted-large.zip")

	found := binarySearch("append-test-01000", dirHeaders, records, recSize)
	if found != 1003 {
		t.Errorf("BinarySearch(key=%q): got %d, want %d", "append-test-01000", found, 1003)
	}

	found = binarySearch("append-test-20000", dirHeaders, records, recSize)
	if found != 20003 {
		t.Errorf("BinarySearch(key=%q): got %d, want %d", "append-test-20000", found, 20003)
	}

	found = binarySearch("append-test-30000", dirHeaders, records, recSize)
	if found != -1 {
		t.Errorf("BinarySearch(key=%q): got %d, want %d", "append-test-30000", found, -1)
	}
}

func TestFindKeys(t *testing.T) {
	dirHeaders, records, recSize := loadZip(t, "testdata/"+"sorted-large.zip")

	keys := []string{
		"append-test-00002",
		"append-test-01234",
		"append-test-02345",
		"not-found",
	}

	fks := FindKeys(keys, dirHeaders, records, recSize)
	for _, fk := range fks {
		switch fk.Name {
		case "append-test-00002":
			if fk.Offset != 0x21a7 {
				t.Errorf("FindKeys(key=%q): got %d, want %d", fk.Name, fk.Offset, 0x21a7)
			}
			if fk.CompressedSize != 0x1000 {
				t.Errorf("FindKeys(key=%q): got %d, want %d", fk.Name, fk.CompressedSize, 0x1000)
			}
		case "append-test-01234":
			if fk.Offset != 0x4e8a97 {
				t.Errorf("FindKeys(key=%q): got %d, want %d", fk.Name, fk.Offset, 0x4e8a97)
			}
			if fk.CompressedSize != 0x1000 {
				t.Errorf("FindKeys(key=%q): got %d, want %d", fk.Name, fk.CompressedSize, 0x1000)
			}
		case "append-test-02345":
			if fk.Offset != 0x954014 {
				t.Errorf("FindKeys(key=%q): got %d, want %d", fk.Name, fk.Offset, 0x954014)
			}
			if fk.CompressedSize != 0x1000 {
				t.Errorf("FindKeys(key=%q): got %d, want %d", fk.Name, fk.CompressedSize, 0x1000)
			}
		}
	}
}

func BenchmarkBinarySearch(b *testing.B) {
	dirHeaders, records, recSize := loadZip(b, "testdata/"+"sorted-large.zip")
	const pattern = "append-test-%05d"

	for n := 0; n < b.N; n++ {
		key := fmt.Sprintf(pattern, rand.Intn(records-3))
		binarySearch(key, dirHeaders, records, recSize)
	}
}

func dumpDirHeaders(dirHeaders []byte, recSize int, records int) {
	for o := 0; o < len(dirHeaders); o += recSize {
		if o/recSize > 3 && records-o/recSize > 3 {
			continue
		}
		dirHeader := dirHeader(dirHeaders[o : o+recSize])
		fmt.Println(dirHeader.Name())
	}
}
