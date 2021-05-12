package chidb

import (
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

	// Offset where to start to read from data
	offset uint16

	// Page bytes data
	// TODO: change this to fixed size list
	data []byte
}

type Pager struct {
	buffer     *os.File
	totalPages uint32
}

// Open opens a file for paged access
func Open(filename string) (*Pager, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModePerm)
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
	if _, err := p.buffer.Seek(io.SeekStart, 0); err != nil {
		return nil, err
	}

	header := make([]byte, HeaderSize)
	if _, err := p.buffer.Read(header); err != nil {
		return nil, err
	}

	return header, nil
}

func (p *Pager) WriteHeader(header []byte) error {
	if _, err := p.buffer.Seek(io.SeekStart, 0); err != nil {
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

	if err := p.seekPage(page); err != nil {
		return nil, err
	}

	data := make([]byte, 0, PageSize)
	count, err := p.buffer.Read(data)
	if err != nil {
		return nil, err
	}
	log.Printf("Read %d bytes from page %d\n", count, page)

	// Page one is special, the first HEADER_SIZE are used by the header
	// so we start to read after the header.
	// http://chi.cs.uchicago.edu/chidb/fileformat.html#physical-organization
	offset := uint16(0)
	if page == 1 {
		offset = HeaderSize + 1
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
func (p *Pager) WritePage(page MemPage) error {
	if err := p.pageIsValid(page.number); err != nil {
		return err
	}

	if err := p.seekPage(page.number); err != nil {
		return err
	}

	if l := len(page.data); l != PageSize {
		return fmt.Errorf("invalid page data size %d", l)
	}

	count, err := p.buffer.Write(page.data)
	if err != nil {
		return err
	}
	log.Printf("Wrote %d bytes to page %d\n", count, page.number)

	return nil
}

// AllocatePage Allocate an extra page on the file and returns the page number
func (p *Pager) AllocatePage() uint32 {
	// We simply increment the page number counter.
	// read_page and write_page take care of the rest.
	//
	// TODO: We should create an empty page here???
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

func (p *Pager) pageIsValid(page uint32) error {
	if page > p.totalPages || page <= 0 {
		return ErrIncorrectPageNumber
	}
	return nil
}

func (p *Pager) seekPage(page uint32) error {
	seek := (page - 1) * PageSize
	if _, err := p.buffer.Seek(io.SeekStart, int(seek)); err != nil {
		return err
	}
	return nil
}
