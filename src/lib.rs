#![allow(dead_code)]
use bytes::{BufMut, BytesMut};
use std::io::{
    self,
    prelude::{Read, Write},
    BufReader, BufWriter,
};
use std::mem::size_of;
use std::path::Path;

pub mod pager;

use pager::{Pager, HEADER_SIZE, PAGE_SIZE};

#[derive(PartialEq)]
pub enum ChiError {
    /// The page has an incorrect page number
    EPageNo,

    /// The file does not have a header
    NoHeader,

    /// Database file contains an invalid header
    Ecorruptheader,

    /// Could not allocate memory
    Enomem,

    /// An I/O error
    IO(io::ErrorKind),
}

impl std::fmt::Debug for ChiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChiError::EPageNo => write!(f, "page has an incorrect page number"),
            ChiError::NoHeader => write!(f, "file does not have a header"),
            ChiError::Ecorruptheader => write!(f, "invalid database header"),
            ChiError::Enomem => write!(f, "could not allocate memory"),
            ChiError::IO(err) => write!(f, "{:?}", err),
        }
    }
}

impl From<io::Error> for ChiError {
    fn from(err: io::Error) -> Self {
        Self::IO(err.kind())
    }
}

/// The BTree struct represent a "B-Tree file". It contains a pointer to the
/// chidb database it is a part of, and a pointer to a Pager, which it will
/// use to access pages on the file
#[derive(Debug)]
pub struct BTree {
    pager: Pager,
}

impl BTree {
    /// Open a B-Tree file
    ///
    /// This function opens a database file and verifies that the file
    /// header is correct. If the file is empty (which will happen
    /// if the pager is given a filename for a file that does not exist)
    /// then this function will (1) initialize the file header using
    /// the default page size and (2) create an empty table leaf node
    /// in page 1.
    ///
    /// Parameters
    /// - filename: Database file (might not exist)
    pub fn open(filename: &Path) -> Result<Self, ChiError> {
        let pager = Pager::open(filename)?;

        if pager.is_empty()? {
            let mut btree = BTree { pager };
            btree.initialize_header()?;
            Ok(btree)
        } else {
            let mut btree = BTree { pager };
            btree.validate_header()?;
            Ok(btree)
        }
    }

    fn validate_header(&mut self) -> Result<(), ChiError> {
        let header = self.read_header()?;
        if MAGIC_BYTES.clone() != header.magic_bytes {
            Err(ChiError::Ecorruptheader)
        } else {
            Ok(())
        }
    }

    fn initialize_header(&mut self) -> Result<(), ChiError> {
        let header = BTreeHeader::default();

        let n_page = self.pager.allocate_page();
        let mut page = self.pager.read_page(n_page)?;

        let mut bytes = BytesMut::with_capacity(page.data.len());
        bytes.extend_from_slice(&page.data);

        let raw = &mut bytes[0..HEADER_SIZE];
        raw.copy_from_slice(&header.to_bytes()?);

        // TODO: avoid a lot of copies
        let mut new_data = [0; PAGE_SIZE];
        new_data.copy_from_slice(&bytes[..]);
        page.data = new_data;

        self.pager.write_page(&page)?;

        Ok(())
    }

    fn read_header(&mut self) -> Result<BTreeHeader, ChiError> {
        let header_bytes = self.pager.read_header()?;
        let header = BTreeHeader::from_bytes(&header_bytes)?;
        Ok(header)
    }
}

const MAGIC_BYTES_SIZE: usize = 15;
const MAGIC_BYTES: &[u8; MAGIC_BYTES_SIZE] = b"SQLite format 3";
const PAGE_CACHE_SIZE_INITIAL: usize = 20000;

struct BTreeHeader {
    /// Magic bytes of binary file
    magic_bytes: [u8; MAGIC_BYTES_SIZE],

    /// Size of database page
    page_size: u16,

    /// Initialized to 0. Each time a modification is made to the database, this counter is increased.
    file_change_counter: u32,

    /// Initialized to 0. Each time the database schema is modified, this counter is increased.
    schema_version: u32,

    /// Default pager cache size in bytes. Initialized to `PAGE_CACHE_SIZE_INITIAL`
    page_cache_size: u32,

    /// Available to the user for read-write access. Initialized to 0
    user_cookie: u32,
}

impl BTreeHeader {
    fn to_bytes(&self) -> io::Result<[u8; HEADER_SIZE]> {
        let bytes = BytesMut::with_capacity(HEADER_SIZE);
        let mut buffer = BufWriter::with_capacity(HEADER_SIZE, bytes.writer());

        let mut bytes_writen = buffer.write(&self.magic_bytes)?;
        bytes_writen += buffer.write(&self.page_size.to_le_bytes())?;
        bytes_writen += buffer.write(&self.file_change_counter.to_le_bytes())?;
        bytes_writen += buffer.write(&self.schema_version.to_le_bytes())?;
        bytes_writen += buffer.write(&self.page_cache_size.to_le_bytes())?;
        bytes_writen += buffer.write(&self.user_cookie.to_le_bytes())?;

        let empty_space = vec![0; HEADER_SIZE - bytes_writen];
        buffer.write(&empty_space)?;

        let mut raw = [0; HEADER_SIZE];
        raw.copy_from_slice(buffer.buffer());

        Ok(raw)
    }

    fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> io::Result<BTreeHeader> {
        let mut buffer = BufReader::new(&bytes[..]);

        let mut magic_bytes = [0; MAGIC_BYTES_SIZE];
        let mut page_size = [0; size_of::<u16>()];
        let mut file_change_counter = [0; size_of::<u32>()];
        let mut schema_version = [0; size_of::<u32>()];
        let mut page_cache_size = [0; size_of::<u32>()];
        let mut user_cookie = [0; size_of::<u32>()];

        buffer.read(&mut magic_bytes)?;
        buffer.read(&mut page_size)?;
        buffer.read(&mut file_change_counter)?;
        buffer.read(&mut schema_version)?;
        buffer.read(&mut page_cache_size)?;
        buffer.read(&mut user_cookie)?;

        Ok(BTreeHeader {
            magic_bytes,
            page_size: u16::from_le_bytes(page_size),
            file_change_counter: u32::from_le_bytes(file_change_counter),
            schema_version: u32::from_le_bytes(schema_version),
            page_cache_size: u32::from_le_bytes(page_cache_size),
            user_cookie: u32::from_le_bytes(user_cookie),
        })
    }
}

impl Default for BTreeHeader {
    fn default() -> Self {
        BTreeHeader {
            magic_bytes: MAGIC_BYTES.clone(),
            page_size: PAGE_SIZE as u16,
            file_change_counter: 0,
            schema_version: 0,
            page_cache_size: PAGE_CACHE_SIZE_INITIAL as u32,
            user_cookie: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{NamedTempFile, TempDir};

    #[test]
    fn test_create_empty_btree() -> Result<(), ChiError> {
        let temp_dir = TempDir::new()?;
        let file = temp_dir.into_path().join("test_create_empty_btree");

        let _ = BTree::open(&file)?;

        Ok(())
    }

    #[test]
    fn test_open_valid_btree() -> Result<(), ChiError> {
        let temp_dir = TempDir::new()?;
        let file = temp_dir.into_path().join("test_open_valid_btree");

        // Assert create empty btree
        let _ = BTree::open(&file)?;

        // Assert open existed btree
        let _ = BTree::open(&file)?;

        Ok(())
    }

    #[test]
    fn test_open_invalid_btree() -> Result<(), ChiError> {
        let invalid_header = [0; pager::HEADER_SIZE];
        let mut file = NamedTempFile::new()?;
        file.write(&invalid_header)?;

        let result = BTree::open(&file.path());
        assert_eq!(result.err(), Some(ChiError::Ecorruptheader));

        Ok(())
    }
}
