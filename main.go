package main

import (
	"encoding/binary"
	"errors"
	"os"
	"path"
)

type BlockID struct {
	filename string
	blknum   int
}

func newBlockID(filename string, blknum int) BlockID {
	return BlockID{
		filename: filename,
		blknum:   blknum,
	}
}

type Page struct {
	buf []byte
}

func newPage(blocksize int) Page {
	return Page{buf: make([]byte, blocksize)}
}

func newPageFromBytes(b []byte) Page {
	return Page{buf: b} // TODO?
}

func byteSliceToInt32(bs [int32size]byte) int32 {
	return int32(binary.BigEndian.Uint32(bs[:]))
}

func int32ToByteSlice(n int32) [int32size]byte {
	var bs [int32size]byte
	binary.BigEndian.PutUint32(bs[:], uint32(n))
	return bs
}

func (p Page) setInt(offset int32, n int32) {
	binary.BigEndian.PutUint32(p.buf[offset:], uint32(n))
}

func (p Page) getInt(offset int32) int32 {
	return int32(binary.BigEndian.Uint32(p.buf[offset:]))
}

// Set the byte slice into the page starting from offset.
// The caller is responsible for checking there's enough space.
func (p Page) setBytes(offset int32, bs []byte) {
	// Write the length of the slice
	n := int32(len(bs))
	p.setInt(offset, n)

	start := offset + int32size
	end := start + n

	copy(p.buf[start:end], bs)
}

// maxLength returns the size of a stored string in the page
// assumes string is ASCII
func maxLength(s string) int32 {
	return int32size + int32(len(s))
}

// Read a byte slice from the page starting from offset.
func (p Page) getBytes(offset int32) []byte {
	// Read the size of the byte slice
	n := p.getInt(offset)

	start := offset + int32size
	end := start + n

	bs := make([]byte, n)
	copy(bs, p.buf[start:end])
	return bs
}

func (p Page) setString(offset int32, s string) {
	p.setBytes(offset, []byte(s))
}

func (p Page) getString(offset int32) string {
	return string(p.getBytes(offset))
}

type FileManager struct {
	directory *os.File
	blocksize int
	files     map[string]*os.File
}

func newFileManager(dir string, blocksize int) FileManager {
	// Open directory, possubly creating it if it doesn't exist
	directory, err := os.Open(dir)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(dir, os.ModePerm)
		if err != nil {
			if !errors.Is(err, os.ErrExist) {
				panic(err)
			}
		}
		// Open the directory again, since it's nil right now
		directory, _ = os.Open(dir)
		return FileManager{
			directory: directory,
			blocksize: blocksize,
			files:     make(map[string]*os.File, 0),
		}
	}
	if err != nil {
		panic(err)
	}
	return FileManager{
		directory: directory,
		blocksize: blocksize,
		files:     make(map[string]*os.File, 0),
	}
}

// Reads block into page
func (fm *FileManager) read(blk BlockID, p Page) {
	f := fm.getFile(blk.filename)
	f.Seek(int64(blk.blknum)*int64(fm.blocksize), 0)
	bytesRead, err := f.Read(p.buf)
	if err != nil {
		panic(err)
	}
	if bytesRead != len(p.buf) {
		panic("mismatch in bytes read?")
	}
}

// write page into block
func (fm *FileManager) write(blk BlockID, p Page) {
	f := fm.getFile(blk.filename)
	f.Seek(int64(blk.blknum*fm.blocksize), 0)
	bytesWritten, err := f.Write(p.buf)
	if err != nil {
		panic(err)
	}
	if bytesWritten != len(p.buf) {
		panic("mismatch in bytes written")
	}
}

// Adds a new (empty) block
//
// In the Java code this is public synchronized
func (fm *FileManager) append(filename string) BlockID {
	newblknum := fm.length(filename)
	blk := newBlockID(filename, newblknum)
	b := make([]byte, fm.blocksize)
	f := fm.getFile(blk.filename)
	f.Seek(int64(blk.blknum*fm.blocksize), 0)
	bytesWritten, err := f.Write(b)
	if err != nil {
		panic(err)
	}
	if bytesWritten != len(b) {
		panic("mismatch in bytes written")
	}
	return blk
}

func (fm *FileManager) length(filename string) int {
	f := fm.getFile(filename)
	stat, err := f.Stat()
	if err != nil {
		panic(err)
	}
	len := int(stat.Size()) / fm.blocksize
	return len
}

func (fm *FileManager) getFile(filename string) *os.File {
	f, ok := fm.files[filename]
	if ok {
		return f
	}

	// File isn't used by the manager, create it (or truncate it)
	p := path.Join(fm.directory.Name(), filename)
	f, err := os.Create(p)
	if err != nil {
		panic(err)
	}
	fm.files[filename] = f
	return f
}

const int32size = 4

func testFileManager() {
	fm := newFileManager("file-manager-test-dir", 400)
	blk := newBlockID("testfile", 2)
	p1 := newPage(fm.blocksize)
	pos1 := int32(88)
	p1.setString(pos1, "abcdefghijklm")
	// account for storing the size of the string as an int32 in the page
	pos2 := pos1 + maxLength("abcdefghijklm")

	n := int32(4096)
	p1.setInt(pos2, n)
	fm.write(blk, p1) // writes the page we've been working on to the block on disk

	// Verify we can read the block back into another page
	p2 := newPage(fm.blocksize)
	fm.read(blk, p2)
	assert(n == p2.getInt(pos2))
	assert("abcdefghijklm" == p2.getString(pos1))
}

func assert(check bool) {
	if !check {
		panic("assertion failed")
	}
}

func main() {
	testFileManager()
}
