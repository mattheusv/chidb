use bytes::BytesMut;
use std::fs::{File, OpenOptions};
use std::io::{
    self,
    prelude::{Read, Seek, Write},
    SeekFrom,
};
use std::path::Path;

use crate::ChiError;

pub const PAGE_SIZE: usize = 4096 * 4; // 8 Kb
pub const HEADER_SIZE: usize = 100;

/// Represents a in-memory copy of page
pub struct MemPage {
    /// Number of physical page
    pub n_page: u32,

    /// Offset where to start to read from data
    pub offset: u16,

    /// Page bytes data
    data: BytesMut,
}

impl MemPage {
    /// Create a new MemPage
    pub fn new(n_page: u32, raw: [u8; PAGE_SIZE], offset: u16) -> Self {
        let mut data = BytesMut::with_capacity(raw.len());
        data.extend_from_slice(&raw);
        MemPage {
            n_page,
            data,
            offset,
        }
    }

    /// Return a reference to the data of page starting from offset
    pub fn data(&self) -> &[u8] {
        &self.data[self.offset as usize..]
    }

    /// Like `data` but return a mutable reference
    pub fn data_as_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.offset as usize..]
    }

    /// Return a reference to all data from page
    pub fn raw(&self) -> &[u8] {
        &self.data[..]
    }

    /// Update raw data of page
    ///
    /// This function override current in-memory data of page
    /// to the new raw data from param taking care to update data
    /// using the offset of page as base.
    pub fn set_data(&mut self, raw: &[u8]) {
        let mut data = BytesMut::with_capacity(self.data.capacity());
        data.extend_from_slice(&self.data[..self.offset as usize]);
        data.extend_from_slice(raw);
        self.data = data;
    }
}

#[derive(Debug)]
pub struct Pager {
    buffer: File,
    total_pages: u32,
}

impl Pager {
    /// Open a file
    //
    // This function opens a file for paged access.
    //
    // Parameters
    // - filename: Database file (might not exist)
    pub fn open(filename: &Path) -> Result<Pager, ChiError> {
        let buffer = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;
        Ok(Pager {
            buffer,
            total_pages: 0,
        })
    }

    /// Read the file header
    ///
    /// This function reads in the header of a chidb file and returns it
    /// in a byte array. Note that this function can be called even if
    /// the page size is unknown, since the chidb header always occupies
    /// the first 100 bytes of the file.
    pub fn read_header(&mut self) -> Result<BytesMut, ChiError> {
        self.buffer.seek(SeekFrom::Start(0))?;
        let mut header = [0; HEADER_SIZE];
        self.buffer.read(&mut header)?;
        let mut bytes = BytesMut::with_capacity(header.len());
        bytes.extend_from_slice(&header);
        Ok(bytes)
    }

    pub fn write_header(&mut self, header: &[u8; HEADER_SIZE]) -> Result<(), ChiError> {
        self.buffer.seek(SeekFrom::Start(0))?;
        self.buffer.write(header)?;
        Ok(())
    }

    /// Allocate an extra page on the file and returns the page number
    pub fn allocate_page(&mut self) -> u32 {
        // We simply increment the page number counter.
        // read_page and write_page take care of the rest.
        //
        // TODO: We should create an empty page here???
        self.total_pages += 1;
        self.total_pages
    }

    /// Read a page from file
    ///
    /// This page reads a page from the file, and creates an in-memory copy
    /// in a MemPage struct (see header file for more details on this struct).
    /// Any changes done to a MemPage will not be effective until you call
    /// chidb_Pager_writePage with that MemPage.
    ///
    /// Parameters
    /// - n_page: Page number of page to read.
    pub fn read_page(&mut self, n_page: u32) -> Result<MemPage, ChiError> {
        if n_page > self.total_pages || n_page <= 0 {
            return Err(ChiError::EPageNo);
        }
        let seek = (n_page - 1) * PAGE_SIZE as u32;
        self.buffer.seek(SeekFrom::Start(seek as u64))?;

        let mut data = [0; PAGE_SIZE];
        let count = self.buffer.read(&mut data)?;
        println!("Read {} bytes from page {}", count, n_page);

        // Page one is special, the first HEADER_SIZE are used by the header
        // so we start to read after the header.
        // http://chi.cs.uchicago.edu/chidb/fileformat.html#physical-organization
        let mut offset = 0;
        if n_page == 1 {
            offset = HEADER_SIZE as u16 + 1;
        }

        Ok(MemPage::new(n_page, data, offset))
    }

    pub fn write_page(&mut self, page: &MemPage) -> Result<(), ChiError> {
        if page.n_page > self.total_pages || page.n_page <= 0 {
            return Err(ChiError::EPageNo);
        }
        let seek = (page.n_page - 1) * PAGE_SIZE as u32;
        self.buffer.seek(SeekFrom::Start(seek as u64))?;
        let count = self.buffer.write(&page.data)?;
        println!("Wrote {} bytes to page {}", count, page.n_page);
        Ok(())
    }

    pub fn is_empty(&self) -> Result<bool, io::Error> {
        let size = self.buffer.metadata()?.len();
        Ok(size == 0)
    }
}
