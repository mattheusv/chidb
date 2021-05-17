package chidb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
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

/// GetNodeByPage Loads a B-Tree node from disk
///
/// Reads a B-Tree node from a page in the disk. All the information regarding
/// the node is stored in a BTreeNode struct (see header file for more details
/// on this struct).
/// Any changes made to a BTreeNode variable will not be effective in the database
/// until write_node is called on that BTreeNode.
func (b *BTree) GetNodeByPage(nPage uint32) (*BTreeNode, error) {
	page, err := b.pager.ReadPage(nPage)
	if err != nil {
		return nil, err
	}
	return BTreeNodeFromPage(page)
}

// NewNode create a new B-Tree node
//
// Allocates a new page in the file and initializes it as an empty B-Tree node.
func (b *BTree) NewNode(typ BTreeNodeType) (*BTreeNode, error) {
	nPage := b.pager.AllocatePage()
	page, err := b.pager.ReadPage(nPage)
	if err != nil {
		return nil, err
	}

	node := NewBTreeNode(page, typ)

	bytes, err := node.Bytes()
	if err != nil {
		return nil, err
	}

	if err := page.Write(bytes); err != nil {
		return nil, err
	}

	if err := b.pager.WritePage(page); err != nil {
		return nil, err
	}

	return node, nil
}

// Initialize a B-Tree node
//
// Initializes a database page to contain an empty B-Tree node. The
// database page is assumed to exist and to have been already allocated
// by the pager.
func (b *BTree) InitEmptyNode(nPage uint32, typ BTreeNodeType) error {
	// FIXME: I don't know how to implement this since NewNode already creates a new empty node
	return errors.New("not implemented")
}

// WriteNode writes an in-memory B-Tree node to disk
//
// Writes an in-memory B-Tree node to disk. To do this, we need to update
// the in-memory page according to the chidb page format. Since the cell
// offset array and the cells themselves are modified directly on the
// page, the only thing to do is to store the values of "type",
// "free_offset", "n_cells", "cells_offset" and "right_page" in the
// in-memory page.
func (b *BTree) WriteNode(node *BTreeNode) error {
	bytes, err := node.Bytes()
	if err != nil {
		return err
	}
	if err := node.page.Write(bytes); err != nil {
		return err
	}

	return b.pager.WritePage(node.page)
}

// Close closes the btree buffer
func (b *BTree) Close() error {
	return b.pager.Close()
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
	if err := page.Write(bytes); err != nil {
		return err
	}
	return b.pager.WritePage(page)
}

func (b *BTree) validateHeader() error {
	header, err := b.ReadHeader()
	if err != nil {
		return err
	}
	if bytes.Equal(header.magicBytes, MagicBytes) {
		return nil
	}
	return ErrCorruptHeader
}

// ReadHeader returns the header values of btree file
func (b *BTree) ReadHeader() (*BTreeHeader, error) {
	bytes, err := b.pager.ReadHeader()
	if err != nil {
		return nil, err
	}
	return NewBtreeHeader(bytes)
}

type BTreeNodeType byte

const (
	InternalTable BTreeNodeType = 0x05
	LeafTable     BTreeNodeType = 0x0D
	InternalIndex BTreeNodeType = 0x02
	LeafIndex     BTreeNodeType = 0x0A
)

// BTreeNodeTypeFromByte create a BTreeNodeType from a raw byte
func BTreeNodeTypeFromByte(b byte) (BTreeNodeType, error) {
	switch b {
	case 0x05:
		return InternalTable, nil
	case 0x0D:
		return LeafTable, nil
	case 0x02:
		return InternalIndex, nil
	case 0x0A:
		return LeafIndex, nil
	}
	return BTreeNodeType(b), fmt.Errorf("invalid btree node type %v", b)
}

// Value return the byte representation of BTreeNodeType
func (n BTreeNodeType) Value() byte {
	return byte(n)
}

func (n BTreeNodeType) String() string {
	switch n {
	case InternalTable:
		return "internal table"
	case LeafTable:
		return "leaf table"
	case InternalIndex:
		return "internal index"
	case LeafIndex:
		return "leaf index"
	}
	return "<invalid type>"
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
	cellOffsetArray byte
}

const PageHeaderSize = 12

// NewBTreeNode create a new BTreeNode with default values
func NewBTreeNode(page *MemPage, typ BTreeNodeType) *BTreeNode {
	return &BTreeNode{
		page:            page,
		typ:             typ,
		freeOffset:      PageHeaderSize + 1,
		cellsOffset:     PageSize,
		cellOffsetArray: PageHeaderSize + 1,
		nCells:          0,
		rightPage:       0,
	}
}

// BTreeNodeFromPage creates a new BTreeNode from MemPage
func BTreeNodeFromPage(page *MemPage) (*BTreeNode, error) {
	var node BTreeNode

	buffer := bytes.NewReader(page.Read())

	freeOffset := make([]byte, unsafe.Sizeof(node.freeOffset))
	nCells := make([]byte, unsafe.Sizeof(node.nCells))
	cellsOffset := make([]byte, unsafe.Sizeof(node.cellsOffset))
	righPage := make([]byte, unsafe.Sizeof(node.rightPage))

	typeBytes, err := buffer.ReadByte()
	if err != nil {
		return nil, err
	}
	if _, err := buffer.Read(freeOffset); err != nil {
		return nil, err
	}
	if _, err := buffer.Read(nCells); err != nil {
		return nil, err
	}
	if _, err := buffer.Read(cellsOffset); err != nil {
		return nil, err
	}
	if _, err := buffer.Read(righPage); err != nil {
		return nil, err
	}
	cellOffsetArray, err := buffer.ReadByte()
	if err != nil {
		return nil, err
	}

	typ, err := BTreeNodeTypeFromByte(typeBytes)
	if err != nil {
		return nil, err
	}

	node.page = page
	node.typ = typ
	node.freeOffset = binary.LittleEndian.Uint16(freeOffset)
	node.nCells = binary.LittleEndian.Uint16(nCells)
	node.cellsOffset = binary.LittleEndian.Uint16(cellsOffset)
	node.rightPage = binary.LittleEndian.Uint16(righPage)
	node.cellOffsetArray = cellOffsetArray

	return &node, nil
}

// GetCell read the contents of a cell
//
// Reads the contents of a cell from a BTreeNode and stores them in a BTreeCell.
// This involves the following:
//  1. Find out the offset of the requested cell in cell offset array.
//  2. Read the cell from the in-memory page, and parse its
//     contents (refer to The chidb File Format document for
//     the format of cells).
func (n *BTreeNode) GetCell(nCell uint16) (*BTreeCell, error) {
	cellsOffset, idx, found := n.getCellOffset(nCell)
	if !found {
		return nil, fmt.Errorf("not found cell %d", nCell)
	}

	buffer := bytes.NewReader(n.page.Read())

	offset := cellsOffset[idx]
	seek := int64(PageHeaderSize + 1 + int(offset))
	if _, err := buffer.Seek(seek, io.SeekStart); err != nil {
		return nil, err
	}

	switch n.typ {
	case InternalTable:
		return nil, fmt.Errorf("not implemeted")
	case LeafTable:
		var cell BTreeCell

		sizeBytes := make([]byte, unsafe.Sizeof(cell.fields.tableLeaf.size))
		key := make([]byte, unsafe.Sizeof(cell.key))

		if _, err := buffer.Read(sizeBytes); err != nil {
			return nil, err
		}
		if _, err := buffer.Read(key); err != nil {
			return nil, err
		}

		size := binary.LittleEndian.Uint32(sizeBytes)

		data := make([]byte, size)
		if _, err := buffer.Read(data); err != nil {
			return nil, err
		}

		cell.fields.tableLeaf.size = size
		cell.fields.tableLeaf.data = data
		cell.key = binary.LittleEndian.Uint32(key)

		return &cell, nil
	case InternalIndex:
		return nil, fmt.Errorf("not implemeted")
	case LeafIndex:
		return nil, fmt.Errorf("not implemeted")
	default:
		return nil, fmt.Errorf("invalid node type %d", n.typ)
	}
}

// InsertCell insert a new cell into a B-Tree node
//
// Inserts a new cell into a B-Tree node at a specified position n_cell.
// This involves the following:
//  1. Add the cell at the top of the cell area. This involves "translating"
//     the BTreeCell into the chidb format (refer to The chidb File Format
//     document for the format of cells).
//  2. Modify cells_offset in BTreeNode to reflect the growth in the cell area.
//  3. Modify the cell offset array so that all values in positions >= ncell
//     are shifted one position forward in the array. Then, set the value of
//     position ncell to be the offset of the newly added cell.
//
// This function assumes that there is enough space for this cell in this node.
func (n *BTreeNode) InsertCell(nCell uint16, cell *BTreeCell) error {
	cellOffsetArray, idx, found := n.getCellOffset(nCell)
	if found {
		return fmt.Errorf("cell %d already exists", nCell)
	}

	bytes, err := cell.Bytes()
	if err != nil {
		return err
	}

	cellOffset := n.cellsOffset - uint16(len(bytes))
	if err := n.page.WriteAt(bytes, cellOffset); err != nil {
		return err
	}

	n.cellsOffset = cellOffset

	nCellBytes := make([]byte, unsafe.Sizeof(nCell))
	binary.LittleEndian.PutUint16(nCellBytes, nCell)

	newCellOffsetArray := make([]byte, 0, len(cellOffsetArray))
	newCellOffsetArray = append(newCellOffsetArray, cellOffsetArray[:idx]...)
	newCellOffsetArray = append(newCellOffsetArray, nCellBytes...)
	newCellOffsetArray = append(newCellOffsetArray, cellOffsetArray[idx:]...)

	if err := n.page.WriteAt(newCellOffsetArray, uint16(n.cellOffsetArray)); err != nil {
		return err
	}

	n.nCells++
	n.freeOffset += n.nCells * uint16(unsafe.Sizeof(nCell))

	return nil
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

	if err := buffer.WriteByte(n.cellOffsetArray); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(make([]byte, n.page.Len()-buffer.Len())); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (b *BTreeNode) Type() BTreeNodeType {
	return b.typ
}

func (n *BTreeNode) getCellOffset(nCell uint16) ([]byte, uint16, bool) {
	data := n.page.Read()
	cellOffsetArray := data[n.cellOffsetArray:n.freeOffset]
	if len(cellOffsetArray) == 0 {
		return cellOffsetArray, 0, false
	}
	idx := sort.Search(int(nCell), func(i int) bool { return uint16(cellOffsetArray[i]) >= nCell })
	return cellOffsetArray, uint16(idx), idx < len(cellOffsetArray) && uint16(cellOffsetArray[idx]) == nCell
}

// BTreeCell is an in-memory representation of a cell.
type BTreeCell struct {
	// Type of page where this cell is contained
	typ BTreeNodeType

	// Key of cell
	key uint32

	fields struct {
		// Represents a table internal cell
		tableInternal struct {
			// Child page with keys
			childPage uint32
		}

		// Represents a table leaf cell
		tableLeaf struct {
			// Number of bytes of data stored in this cell
			size uint32

			// Pointer to in-memory copy of data stored in this cell
			data []byte
		}

		// Represents a index internal cell
		indexInternal struct {
			// Primary key of row where the indexed field is equal to key
			keyPk uint32

			// Child page with keys
			childPage uint32
		}

		// Represents a index leaf cell
		indexLeaf struct {
			// Primary key of row where the indexed field is equal to key
			keyPk uint32
		}
	}
}

func (b *BTreeCell) Bytes() ([]byte, error) {
	buffer := bytes.NewBuffer([]byte(""))

	switch b.typ {
	case InternalTable:
		return nil, fmt.Errorf("not implemented")
	case LeafTable:
		size := make([]byte, unsafe.Sizeof(b.fields.tableLeaf.size))
		key := make([]byte, unsafe.Sizeof(b.key))
		binary.LittleEndian.PutUint32(size, b.fields.tableLeaf.size)
		binary.LittleEndian.PutUint32(key, b.key)

		buffer.Grow(len(size) + len(key) + len(b.fields.tableLeaf.data))
		if _, err := buffer.Write(size); err != nil {
			return nil, err
		}
		if _, err := buffer.Write(key); err != nil {
			return nil, err
		}
		if _, err := buffer.Write(b.fields.tableLeaf.data); err != nil {
			return nil, err
		}
	case InternalIndex:
		return nil, fmt.Errorf("not implemented")
	case LeafIndex:
		return nil, fmt.Errorf("not implemented")
	default:
		return nil, fmt.Errorf("invalid cell type %d", b.typ)
	}

	return buffer.Bytes(), nil
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
