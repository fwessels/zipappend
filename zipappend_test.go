package zipappend

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Load the central directory of the zip file
func loadCentDir(tb testing.TB, fname string) (dirHeaders []byte, records, recSize int) {
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
	if records > 0 {
		recSize = int(size / records)
	}

	if false {
		dumpCentDir(dirHeaders, records, recSize)
	}
	return
}

func loadCentDirStandalone(tb testing.TB, fname string) (dirHeaders []byte, records, recSize int) {
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

	size := eocd.Size()
	records = eocd.Records()

	bufCentDir := make([]byte, uint(stat.Size()))
	var n int
	if n, err = file.ReadAt(bufCentDir, 0); err != nil {
		tb.Fatal(err)
	} else if n < size {
		tb.Fatalf("could not read central directory: %d", n)
	}

	dirHeaders = bufCentDir[:uint(size)]
	if records > 0 {
		recSize = int(size / records)
	}

	if false {
		dumpCentDir(dirHeaders, records, recSize)
	}
	return
}

func loadFiles(tb testing.TB, fname string) (files []byte) {
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

	files = make([]byte, offset)
	var n int
	if n, err = file.ReadAt(files, 0); err != nil {
		tb.Fatal(err)
	} else if n < int(offset) {
		tb.Fatalf("could not read files: %d", n)
	}

	return
}

func statFiles(tb testing.TB, fname string) int64 {
	file, err := os.Open(fname)
	if err != nil {
		tb.Fatal(err)
	}
	defer file.Close()

	var stat os.FileInfo
	if stat, err = file.Stat(); err != nil {
		tb.Fatal(err)
	}
	return stat.Size()
}

func TestBinarySearch(t *testing.T) {
	dirHeaders, records, recSize := loadCentDir(t, "testdata/"+"sorted-large.zip")

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
	dirHeaders, records, recSize := loadCentDir(t, "testdata/"+"sorted-large.zip")

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

func TestDummyZip(t *testing.T) {

	buf := [DirectoryEndLen]byte{}
	copy(buf[:], []byte{0x50, 0x4b, 0x05, 0x06})
	eocd := DirEndRecord(buf[:])
	eocd.SetOffset(0)
	eocd.SetSize(0)
	eocd.SetRecords(0)

	os.WriteFile("dummy.zip", eocd, 0644)
}

func TestPutAndGet(t *testing.T) {
	totalPutOps := uint64(0)
	totalGetOps := uint64(0)
	totalBytes := uint64(0)
	par := func(wg *sync.WaitGroup, archive string) {
		defer wg.Done()
		tow := testAppend_100(t, archive, 300)
		atomic.AddUint64(&totalPutOps, uint64(tow))
		tor, tb := testGet(t, archive, 50, 100)
		atomic.AddUint64(&totalGetOps, uint64(tor))
		atomic.AddUint64(&totalBytes, uint64(tb))
	}
	var wg sync.WaitGroup
	for p := 1; p <= 1; p++ {
		wg.Add(1)
		go par(&wg, fmt.Sprintf("test__%d.zip", p))
	}
	wg.Wait()
	fmt.Println("total PUT-ops:", totalPutOps/1000, "K")
	fmt.Println("total GET-ops:", totalGetOps/1000, "K")
	fmt.Println("total GET-bytes:", totalBytes/1024/1024, "MB")
}

func getFilesName(fname string) string {
	parts := strings.Split(fname, ".")
	return strings.Join(parts, "_files.")
}

func getDirName(fname string) string {
	parts := strings.Split(fname, ".")
	return strings.Join(parts, "_dir.")
}

func testAppend_100(t *testing.T, archive string, batches int) (totalOps int) {
	var buf []byte
	var err error
	if buf, err = os.ReadFile("testdata/" + "dummy.zip"); err != nil {
		t.Fatal(err)
	} else if err = os.WriteFile(getDirName(archive), buf, 0644); err != nil {
		t.Fatal(err)
	} else if err = os.WriteFile(getFilesName(archive), []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	counter := 1
	for i := 0; i < batches; i++ {
		to, _ := appendZipSplit(t, archive, "testdata/"+"append100.zip", &counter)
		totalOps += to
	}
	return
}

func TestAppend300_100(t *testing.T) {
func appendZipSplit(t *testing.T, base, appnd string, counter *int) (totalOps int, err error) {
	patchFilenameInPlace := func(centDir, files []byte, counter *int) {
		for ptr := 0; ptr < len(centDir); {
			name := "append-test-" + fmt.Sprintf("%05d", *counter)

			// patch name in central directory
			dh := dirHeader(centDir[ptr : ptr+directoryHeaderLen])
			dh.SetName(name)

			// patch name in file section of zip file
			fh := fileHeader(files[dh.Offset() : dh.Offset()+fileHeaderLen])
			fh.SetName(name)

			ptr += dh.Len()
			(*counter)++
		}
	}

	baseFilesSize := statFiles(t, getFilesName(base))
	appendCD, appendRecords, _ := loadCentDir(t, appnd)
	appendFiles := loadFiles(t, appnd)
	patchFilenameInPlace(appendCD, appendFiles, counter)

	{
		var f *os.File
		// TODO: test os.Truncate + O_WRONLY | O_APPEND
		if f, err = os.OpenFile(getFilesName(base), os.O_RDWR, 0); err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		if _, err := f.Seek(baseFilesSize, 0); err != nil {
			log.Fatal(err)
		}

		f.Write(appendFiles)
	}

	baseCD, baseRecords, _ := loadCentDirStandalone(t, getDirName(base))
	mergedCD := AppendSplit(appendCD, uint(baseFilesSize))

	{
		var fdir *os.File

		// TODO: test os.Truncate + O_WRONLY | O_APPEND
		if fdir, err = os.OpenFile(getDirName(base), os.O_RDWR, 0); err != nil {
			log.Fatal(err)
		}
		defer fdir.Close()

		if _, err := fdir.Seek(int64(len(baseCD)), 0); err != nil {
			log.Fatal(err)
		}

		buf := [DirectoryEndLen]byte{}
		copy(buf[:], []byte{0x50, 0x4b, 0x05, 0x06})
		eocd := DirEndRecord(buf[:])
		eocd.SetOffset(uint(int(baseFilesSize) + len(appendFiles)))
		eocd.SetSize(len(baseCD) + len(mergedCD))
		eocd.SetRecords(baseRecords + appendRecords)

		fdir.Write(mergedCD)
		fdir.Write(eocd)
	}
	return appendRecords, nil
}

func appendZip(t *testing.T, base, appnd string, counter *int) (totalOps int, err error) {
	patchFilenameInPlace := func(centDir, files []byte, counter *int) {
		for ptr := 0; ptr < len(centDir); {
			name := "append-test-" + fmt.Sprintf("%05d", *counter)

			// patch name in central directory
			dh := dirHeader(centDir[ptr : ptr+directoryHeaderLen])
			dh.SetName(name)

			// patch name in file section of zip file
			fh := fileHeader(files[dh.Offset() : dh.Offset()+fileHeaderLen])
			fh.SetName(name)

			ptr += dh.Len()
			(*counter)++
		}
	}

	baseCD, baseRecords, _ := loadCentDir(t, base)
	baseFiles := loadFiles(t, base)
	appendCD, appendRecords, _ := loadCentDir(t, appnd)
	appendFiles := loadFiles(t, appnd)
	patchFilenameInPlace(appendCD, appendFiles, counter)

	mergedCD := Append(baseCD, appendCD, uint(len(baseFiles)))

	var f *os.File
	// TODO: test os.Truncate + O_WRONLY | O_APPEND
	if f, err = os.OpenFile(base, os.O_RDWR, 0); err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if _, err := f.Seek(int64(len(baseFiles)), 0); err != nil {
		log.Fatal(err)
	}

	f.Write(appendFiles)

	buf := [DirectoryEndLen]byte{}
	copy(buf[:], []byte{0x50, 0x4b, 0x05, 0x06})
	eocd := DirEndRecord(buf[:])
	eocd.SetOffset(uint(len(baseFiles) + len(appendFiles)))
	eocd.SetSize(len(mergedCD))
	eocd.SetRecords(baseRecords + appendRecords)

	f.Write(mergedCD)
	f.Write(eocd)
	return appendRecords, nil
}

func TestGet1000_100(t *testing.T) {
	testGet(t, "testdata/"+"sorted-large.zip", 1000, 100)
}

func TestGet10000_10(t *testing.T) {
	testGet(t, "testdata/"+"sorted-large.zip", 10000, 10)
}

func TestGet100000_1(t *testing.T) {
	testGet(t, "testdata/"+"sorted-large.zip", 100000, 1)
}

func testGet(t *testing.T, archive string, batches, batchSize int) (totalOps, totalBytes int) {

	for run := 0; run < batches; run++ {
		dirHeaders, records, recSize := loadCentDir(t, archive)
		const pattern = "append-test-%05d"

		{
			keys := make([]string, 0, batchSize)

			for i := 0; i < batchSize; i++ {
				key := fmt.Sprintf(pattern, 1+rand.Intn(records-1))
				keys = append(keys, key)
			}

			var f *os.File
			var err error
			if f, err = os.OpenFile(archive, os.O_RDONLY, 0); err != nil {
				log.Fatal(err)
			}

			b := make([]byte, 0x1000)
			fks := FindKeys(keys, dirHeaders, records, recSize)
			for _, fk := range fks {
				var n int
				if n, err = f.ReadAt(b, int64(fk.Offset)); err != nil {
					log.Fatal(err)
				}
				if n < int(fk.CompressedSize) {
					log.Fatal("could not read")
				}
				// fmt.Println("read: ", n, "for", fk.Name)
				totalBytes += n
				totalOps++
			}

			f.Close()
		}
	}

	// fmt.Println("  totalOps:", totalOps/1000, "K")
	// fmt.Println("totalBytes:", totalBytes/1024/1024, "MB")

	return
}

func BenchmarkBinarySearch(b *testing.B) {
	dirHeaders, records, recSize := loadCentDir(b, "testdata/"+"sorted-large.zip")
	const pattern = "append-test-%05d"

	for n := 0; n < b.N; n++ {
		key := fmt.Sprintf(pattern, rand.Intn(records-3))
		binarySearch(key, dirHeaders, records, recSize)
	}
}

// Dump the central directory records.
func dumpCentDir(dirHeaders []byte, records int, recSize int) {
	for o := 0; o < len(dirHeaders); o += recSize {
		if o/recSize > 3 && records-o/recSize > 3 {
			continue
		}
		dirHeader := dirHeader(dirHeaders[o : o+recSize])
		fmt.Println(dirHeader.Name())
	}
}
