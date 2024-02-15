package zipappend

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func loadZip(buf []byte) (dirHeaders []byte, records, recSize int) {
	eocd := dirEndRecord(buf[len(buf)-directoryEndLen:])

	offset := eocd.Offset()
	size := eocd.Size()
	records = eocd.Records()

	// eodl64 := buf[len(buf)-directory64LocLen-directoryEndLen : len(buf)-directoryEndLen]
	// fmt.Println(hex.Dump(eodl64))

	// eocd64 := buf[len(buf)-directory64EndLen-directory64LocLen-directoryEndLen : len(buf)-directory64LocLen-directoryEndLen]
	// fmt.Println(hex.Dump(eocd64))

	// fmt.Printf(" offset = %08x\n", offset)
	// fmt.Printf("   size = %08x\n", size)
	// fmt.Printf("records = %0d\n", records)

	dirHeaders = buf[offset : offset+uint(size)]
	recSize = int(size / records)

	if false {
		dumpDirHeaders(dirHeaders, recSize, records)
	}

	return
}

func TestFindKey(t *testing.T) {
	buf, err := os.ReadFile("testdata/" + "sorted-large.zip")
	if err != nil {
		t.Fatal(err)
	}

	dirHeaders, records, recSize := loadZip(buf)

	found := binarySearch("append-test-01000", dirHeaders, records, recSize)
	if found != 1003 {
		t.Errorf("FindKey(key=%q): got %d, want %d", "append-test-01000", found, 1003)
	}

	found = binarySearch("append-test-20000", dirHeaders, records, recSize)
	if found != 20003 {
		t.Errorf("FindKey(key=%q): got %d, want %d", "append-test-20000", found, 20003)
	}

	found = binarySearch("append-test-30000", dirHeaders, records, recSize)
	if found != -1 {
		t.Errorf("FindKey(key=%q): got %d, want %d", "append-test-30000", found, -1)
	}
}

func BenchmarkFindKey(b *testing.B) {
	buf, err := os.ReadFile("testdata/" + "sorted-large.zip")
	if err != nil {
		b.Fatal(err)
	}

	const pattern = "append-test-%05d"
	dirHeaders, records, recSize := loadZip(buf)

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
