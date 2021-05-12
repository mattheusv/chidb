package chidb

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

var MagicBytes = []byte("SQLite format 3")

const PageCacheSizeInitial = 20000

type BTreeHeader struct {
	// Magic bytes of binary file
	magicBytes []byte

	// Size of database page
	pageSize uint16

	// Initialized to 0. Each time a modification is made to the database, this counter is increased.
	fileChangeCounter uint32

	// Initialized to 0. Each time the database schema is modified, this counter is increased.
	schemaVersion uint32

	// Default pager cache size in bytes. Initialized to PageCacheSizeInitial
	pageCacheSize uint32

	// Available to the user for read-write access. Initialized to 0
	userCookie uint32
}

func DefaultBTreeHeader() BTreeHeader {
	return BTreeHeader{
		magicBytes:        MagicBytes,
		pageSize:          PageSize,
		pageCacheSize:     PageCacheSizeInitial,
		fileChangeCounter: 0,
		schemaVersion:     0,
		userCookie:        0,
	}
}

func (b *BTreeHeader) Bytes() ([]byte, error) {
	buffer := bytes.NewBuffer([]byte(""))

	pageSize := make([]byte, unsafe.Sizeof(b.pageSize))
	fileChangeCounter := make([]byte, unsafe.Sizeof(b.fileChangeCounter))
	schemaVersion := make([]byte, unsafe.Sizeof(b.schemaVersion))
	pageCacheSize := make([]byte, unsafe.Sizeof(b.pageCacheSize))
	userCookie := make([]byte, unsafe.Sizeof(b.userCookie))

	binary.LittleEndian.PutUint16(pageSize, b.pageSize)
	binary.LittleEndian.PutUint32(fileChangeCounter, b.fileChangeCounter)
	binary.LittleEndian.PutUint32(schemaVersion, b.schemaVersion)
	binary.LittleEndian.PutUint32(pageCacheSize, b.pageCacheSize)
	binary.LittleEndian.PutUint32(userCookie, b.userCookie)

	var (
		bytesWriten int
		err         error
	)

	w, err := buffer.Write(b.magicBytes)
	if err != nil {
		return nil, err
	}
	bytesWriten += w

	w, err = buffer.Write(pageSize)
	if err != nil {
		return nil, err
	}
	bytesWriten += w

	w, err = buffer.Write(fileChangeCounter)
	if err != nil {
		return nil, err
	}
	bytesWriten += w

	w, err = buffer.Write(schemaVersion)
	if err != nil {
		return nil, err
	}
	bytesWriten += w

	w, err = buffer.Write(pageCacheSize)
	if err != nil {
		return nil, err
	}
	bytesWriten += w

	w, err = buffer.Write(userCookie)
	if err != nil {
		return nil, err
	}
	bytesWriten += w

	if _, err := buffer.Write(make([]byte, HeaderSize-bytesWriten)); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
