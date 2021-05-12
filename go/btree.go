package chidb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"
)

const PageCacheSizeInitial = 20000

var MagicBytes = []byte("SQLite format 3")

var ErrCorruptHeader = errors.New("corrupt header")

// BTree represent a "B-Tree file". It contains a pointer to the
// chidb database it is a part of, and a pointer to a Pager, which it will
// use to access pages on the file
type BTree struct {
	pager *Pager
}

// Open a B-Tree file
//
// This function opens a database file and verifies that the file
// header is correct. If the file is empty (which will happen
// if the pager is given a filename for a file that does not exist)
// then this function will (1) initialize the file header using
// the default page size and (2) create an empty table leaf node
// in page 1.
func Open(filename string) (*BTree, error) {
	pager, err := OpenPager(filename)
	if err != nil {
		return nil, err
	}
	btree := &BTree{pager: pager}

	isEmpty, err := pager.IsEmpty()
	if err != nil {
		return nil, err
	}

	if isEmpty {
		if err := btree.initializeHeader(); err != nil {
			return nil, err
		}

		if err := btree.initializeEmptyTableLeaf(); err != nil {
			return nil, err
		}
		return btree, nil
	}

	return btree, btree.validateHeader()
}

func (b *BTree) initializeHeader() error {
	header := DefaultBTreeHeader()
	bytes, err := header.Bytes()
	if err != nil {
		return err
	}
	return b.pager.WriteHeader(bytes)
}

func (b *BTree) initializeEmptyTableLeaf() error {
	nPage := b.pager.AllocatePage()
	page, err := b.pager.ReadPage(nPage)
	if err != nil {
		return err
	}
	node := NewBTreeNode(page, LeafTable)
	bytes, err := node.Bytes()
	if err != nil {
		return err
	}
	return page.Write(bytes)
}

func (b *BTree) validateHeader() error {
	header, err := b.readHeader()
	if err != nil {
		return err
	}
	if bytes.Equal(header.magicBytes, MagicBytes) {
		return nil
	}
	return ErrCorruptHeader
}

func (b *BTree) readHeader() (*BTreeHeader, error) {
	bytes, err := b.pager.ReadHeader()
	if err != nil {
		return nil, err
	}
	return NewBtreeHeader(bytes)
}

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

func NewBtreeHeader(b []byte) (*BTreeHeader, error) {
	var header BTreeHeader

	buffer := bytes.NewReader(b)

	magicBytes := make([]byte, len(MagicBytes))
	pageSize := make([]byte, unsafe.Sizeof(header.pageSize))
	fileChangeCounter := make([]byte, unsafe.Sizeof(header.fileChangeCounter))
	schemaVersion := make([]byte, unsafe.Sizeof(header.schemaVersion))
	pageCacheSize := make([]byte, unsafe.Sizeof(header.pageCacheSize))
	userCookie := make([]byte, unsafe.Sizeof(header.userCookie))

	if _, err := buffer.Read(magicBytes); err != nil {
		return nil, err
	}

	if _, err := buffer.Read(pageSize); err != nil {
		return nil, err
	}
	if _, err := buffer.Read(fileChangeCounter); err != nil {
		return nil, err
	}
	if _, err := buffer.Read(schemaVersion); err != nil {
		return nil, err
	}
	if _, err := buffer.Read(pageCacheSize); err != nil {
		return nil, err
	}
	if _, err := buffer.Read(userCookie); err != nil {
		return nil, err
	}

	header.magicBytes = magicBytes
	header.pageSize = binary.LittleEndian.Uint16(pageSize)
	header.fileChangeCounter = binary.LittleEndian.Uint32(fileChangeCounter)
	header.schemaVersion = binary.LittleEndian.Uint32(schemaVersion)
	header.pageCacheSize = binary.LittleEndian.Uint32(pageCacheSize)
	header.userCookie = binary.LittleEndian.Uint32(userCookie)

	return &header, nil
}

type BTreeNodeType byte

const (
	InternalTable BTreeNodeType = 0x05
	LeafTable     BTreeNodeType = 0x0D
	InternalIndex BTreeNodeType = 0x02
	LeafIndex     BTreeNodeType = 0x0A
)

func (n BTreeNodeType) Value() byte {
	return byte(n)
}

// BTreeNode struct is an in-memory representation of a B-Tree node. Thus,
// most of the values in this struct are simply a copy, for ease of access,
// of what can be found in the raw disk page. When modifying type, free_offset,
// n_cells, cells_offset, or right_page, do so in the corresponding field
// of the BTreeNode variable (the changes will be effective once the BTreeNode
// is written to disk). Modifications of the
// cell offset array or of the cells should be done directly on the in-memory
// page returned by the Pager.
type BTreeNode struct {
	// In-memory page returned by the Pager
	page *MemPage

	// The type of page
	typ BTreeNodeType

	// The byte offset at which the free space starts.
	// Note that this must be updated every time the cell offset array grows.
	freeOffset uint16

	// The number of cells stored in this page.
	nCells uint16

	// The byte offset at which the cells start. If the page contains no cells, this field contains the value PageSize.
	// This value must be updated every time a cell is added.
	cellsOffset uint16

	// Right page (internal nodes only)
	rightPage uint16

	// Pointer to start of cell offset array in the in-memory page
	celloffsetArray byte
}

const PageHeaderSize = 12

func NewBTreeNode(page *MemPage, typ BTreeNodeType) *BTreeNode {
	return &BTreeNode{
		page:            page,
		typ:             typ,
		freeOffset:      PageHeaderSize + 1,
		cellsOffset:     PageSize,
		celloffsetArray: PageHeaderSize,
		nCells:          0,
		rightPage:       0,
	}
}

func (n *BTreeNode) Bytes() ([]byte, error) {
	buffer := bytes.NewBuffer([]byte(""))
	buffer.Grow(PageSize)

	freeOffset := make([]byte, unsafe.Sizeof(n.freeOffset))
	nCells := make([]byte, unsafe.Sizeof(n.nCells))
	cellsOffset := make([]byte, unsafe.Sizeof(n.cellsOffset))
	righPage := make([]byte, unsafe.Sizeof(n.rightPage))

	binary.LittleEndian.PutUint16(freeOffset, n.freeOffset)
	binary.LittleEndian.PutUint16(nCells, n.nCells)
	binary.LittleEndian.PutUint16(cellsOffset, n.cellsOffset)
	binary.LittleEndian.PutUint16(righPage, n.rightPage)

	if err := buffer.WriteByte(n.typ.Value()); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(freeOffset); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(nCells); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(cellsOffset); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(righPage); err != nil {
		return nil, err
	}

	if err := buffer.WriteByte(n.celloffsetArray); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(make([]byte, n.page.Len()-buffer.Len())); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (b *BTreeHeader) Bytes() ([]byte, error) {
	buffer := bytes.NewBuffer([]byte(""))
	buffer.Grow(HeaderSize)

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

	if _, err := buffer.Write(b.magicBytes); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(pageSize); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(fileChangeCounter); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(schemaVersion); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(pageCacheSize); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(userCookie); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(make([]byte, HeaderSize-buffer.Len())); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
