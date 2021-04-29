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
    pub n_page: u32,
    pub data: [u8; PAGE_SIZE],
}

impl MemPage {
    pub fn new(n_page: u32, data: [u8; PAGE_SIZE]) -> Self {
        MemPage { n_page, data }
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
    pub fn read_header(&mut self) -> Result<[u8; HEADER_SIZE], ChiError> {
        self.buffer.seek(SeekFrom::Start(0))?;
        let mut header = [0; HEADER_SIZE];
        let count = self.buffer.read(&mut header)?;
        if count != HEADER_SIZE {
            Err(ChiError::NoHeader)
        } else {
            Ok(header)
        }
    }

    /// Allocate an extra page on the file and returns the page number
    pub fn allocate_page(&mut self) -> u32 {
        // We simply increment the page number counter.
        // read_page and write_page take care of the rest.
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

        Ok(MemPage::new(n_page, data))
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

    /// Check if file is empty
    pub fn is_empty(&self) -> Result<bool, io::Error> {
        let size = self.buffer.metadata()?.len();
        Ok(size == 0)
    }
}
