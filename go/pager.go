package chidb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

const (
	PageSize   = 4096 * 4 // 8 Kb
	HeaderSize = 100
)

var ErrIncorrectPageNumber = errors.New("incorrect page number")

// MemPage Represents a in-memory copy of page
type MemPage struct {

	// Number of physical number
	number uint32

	// Offset where to start to read or write on data
	offset uint16

	// Page bytes data
	data [PageSize]byte
}

// Read returns the bytes of the page
// The returned data is only data avaliable to write and read in page
func (m *MemPage) Read() []byte {
	return m.data[m.offset:]
}

// WriteAt write data on page after at value
func (m *MemPage) WriteAt(data []byte, at uint16) error {
	buffer := bytes.NewBuffer([]byte(""))
	buffer.Grow(PageSize)

	dataSize := uint16(len(m.data))

	if l := dataSize; l < at {
		return fmt.Errorf("page data %d is less than %d", l, at)
	}

	// Write data that is before of `at` value
	if _, err := buffer.Write(m.data[:at]); err != nil {
		return err
	}

	// Write new data
	writen, err := buffer.Write(data)
	if err != nil {
		return err
	}

	remaning := at + uint16(writen)

	if remaning < dataSize {
		// Write the remaning bytes
		if _, err := buffer.Write(m.data[remaning:]); err != nil {
			return err
		}
	}

	if uint16(buffer.Len()) != dataSize {
		fmt.Printf("Buffer len: %d\n", buffer.Len())
		fmt.Printf("Page len: %d\n", dataSize)
		fmt.Println("-----------------------")
		panic("something goes really wrong here")
	}

	newData := buffer.Bytes()
	copy(m.data[:], newData[:PageSize])

	return nil
}

// Write write data on current page
// NOTE: the data param should has the same size of PageSize
func (m *MemPage) Write(data []byte) error {
	buffer := bytes.NewBuffer([]byte(""))
	buffer.Grow(PageSize)

	if _, err := buffer.Write(m.data[:m.offset]); err != nil {
		return err
	}

	if _, err := buffer.Write(data); err != nil {
		return err
	}

	if l := buffer.Len(); l != PageSize {
		return fmt.Errorf("invalid page size to write: expected %d got %d", PageSize, l)
	}

	newData := buffer.Bytes()
	copy(m.data[:], newData[:PageSize])

	return nil
}

// Len returns the lenght of page data available to read and write
func (m *MemPage) Len() int {
	return len(m.Read())
}

type Pager struct {
	buffer     *os.File
	totalPages uint32
}

// OpenPager opens a file for paged access
func OpenPager(filename string) (*Pager, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &Pager{
		buffer:     f,
		totalPages: 0,
	}, nil
}

// ReadHeader reads in the header of a chidb file and returns it
// in a byte array. Note that this function can be called even if
// the page size is unknown, since the chidb header always occupies
// the first 100 bytes of the file.
func (p *Pager) ReadHeader() ([]byte, error) {
	if _, err := p.buffer.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	header := make([]byte, HeaderSize)
	if _, err := p.buffer.Read(header); err != nil {
		return nil, err
	}

	return header, nil
}

func (p *Pager) WriteHeader(header []byte) error {
	if _, err := p.buffer.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if l := len(header); l != HeaderSize {
		return fmt.Errorf("invalid header size %d", l)
	}

	if _, err := p.buffer.Write(header); err != nil {
		return err
	}
	return nil
}

// ReadPage read a page from file
// This page reads a page from the file, and creates an in-memory copy
// in a MemPage struct (see header file for more details on this struct).
// Any changes done to a MemPage will not be effective until you call
// chidb_Pager_writePage with that MemPage.
func (p *Pager) ReadPage(page uint32) (*MemPage, error) {
	if err := p.pageIsValid(page); err != nil {
		return nil, err
	}

	var data [PageSize]byte
	count, err := p.buffer.ReadAt(data[:], p.offset(page))
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read buffer: %w", err)
		}
	}
	log.Printf("Read %d bytes from page %d\n", count, page)

	// Page one is special, the first `HeaderSize` are used by the header
	// so we start to read after the header.
	// http://chi.cs.uchicago.edu/chidb/fileformat.html#physical-organization
	offset := uint16(0)
	if page == 1 {
		offset = HeaderSize
	}

	return &MemPage{
		number: page,
		data:   data,
		offset: offset,
	}, nil
}

// WritePage write a page to file
// This page writes the in-memory copy of a page (stored in a MemPage
// struct) back to disk.
func (p *Pager) WritePage(page *MemPage) error {
	if err := p.pageIsValid(page.number); err != nil {
		return err
	}

	if l := len(page.data); l != PageSize {
		return fmt.Errorf("invalid page data size: expected %d got %d", PageSize, l)
	}

	offset := p.offset(page.number)
	count, err := p.buffer.WriteAt(page.data[:], offset)
	if err != nil {
		return err
	}
	log.Printf("Wrote %d bytes to page %d\n", count, page.number)

	return nil
}

// AllocatePage Allocate an extra page on the file and returns the page number
func (p *Pager) AllocatePage() uint32 {
	// We simply increment the page number counter.
	// ReadPage and WritePage take care of the rest.
	p.totalPages += 1
	return p.totalPages
}

func (p *Pager) IsEmpty() (bool, error) {
	info, err := p.buffer.Stat()
	if err != nil {
		return false, err
	}
	return info.Size() == 0, nil
}

func (p *Pager) Close() error {
	return p.buffer.Close()
}

func (p *Pager) pageIsValid(page uint32) error {
	if page > p.totalPages || page <= 0 {
		return ErrIncorrectPageNumber
	}
	return nil
}

func (p *Pager) offset(page uint32) int64 {
	return int64((page - 1) * PageSize)
}
