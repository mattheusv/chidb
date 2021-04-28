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

#[derive(Debug)]
pub struct Pager {
    buffer: File,

    total_pages: u32,

    page_size: usize,
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
            page_size: PAGE_SIZE,
        })
    }

    /// Read the file header
    ///
    /// This function reads in the header of a chidb file and returns it
    /// in a byte array. Note that this function can be called even if
    /// the page size is unknown, since the chidb header always occupies
    /// the first 100 bytes of the file.
    pub fn read_header(&mut self) -> Result<[u8; HEADER_SIZE], ChiError> {
        let buffer_size = self.buffer.metadata()?.len();
        if buffer_size == 0 {
            return Err(ChiError::NoHeader);
        }

        self.buffer.seek(SeekFrom::Start(0))?;
        let mut header = [0; HEADER_SIZE];
        self.buffer.read(&mut header)?;
        Ok(header)
    }

    pub fn is_empty(&self) -> Result<bool, io::Error> {
        let size = self.buffer.metadata()?.len();
        Ok(size == 0)
    }

    // TODO: remove pub
    pub fn write_buffer(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut bytes_writen = 0;

        while bytes_writen < bytes.len() {
            let writen = self.buffer.write(&bytes[bytes_writen..])?;
            bytes_writen += writen;
        }

        Ok(bytes_writen)
    }
}
