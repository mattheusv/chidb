#![allow(dead_code)]
use std::fs::File;
use std::io::{
    self,
    prelude::{Read, Seek, Write},
    BufReader, BufWriter, SeekFrom,
};
use std::mem::size_of;
use std::path::Path;
use std::str;

pub enum BTreeError {
    /// Database file contains an invalid header
    Ecorruptheader,

    /// Could not allocate memory
    Enomem,

    /// An I/O error
    IO(io::Error),
}

impl From<io::Error> for BTreeError {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}

pub struct BTree {
    buffer: File,
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
    pub fn open(filename: &str) -> Result<Self, BTreeError> {
        let path = Path::new(filename);
        if path.exists() {
            Self::load_from_file(path)
        } else {
            Self::create(path)
        }
    }

    fn create(filename: &Path) -> Result<Self, BTreeError> {
        let file = File::create(filename)?;
        let btree = BTree { buffer: file };
        Ok(btree)
    }

    fn load_from_file(filename: &Path) -> Result<Self, BTreeError> {
        let file = File::open(filename)?;
        let mut btree = BTree { buffer: file };

        if !btree.validate_header()? {
            Err(BTreeError::Ecorruptheader)
        } else {
            Ok(btree)
        }
    }

    fn validate_header(&mut self) -> io::Result<bool> {
        let header = self.header()?;
        let magic_bytes = str::from_utf8(&header.magic_bytes).unwrap_or("");
        Ok(magic_bytes == MAGIC_BYTES)
    }

    fn header(&mut self) -> io::Result<BTreeHeader> {
        self.buffer.seek(SeekFrom::Start(0))?;
        let mut header = [0; HEADER_SIZE];
        self.buffer.read(&mut header)?;
        BTreeHeader::from_bytes(&header)
    }
}

const MAGIC_BYTES: &str = "SQLite format 3";
const MAGIB_BYTES_SIZE: usize = MAGIC_BYTES.len();
const HEADER_SIZE: usize = 100;
const PAGE_CACHE_SIZE_INITIAL: usize = 20000;

struct BTreeHeader {
    /// Magic bytes of binary file
    magic_bytes: [u8; MAGIB_BYTES_SIZE],

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
    fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> io::Result<BTreeHeader> {
        let mut buffer = BufReader::new(&bytes[..]);

        let mut magic_bytes = [0; MAGIB_BYTES_SIZE];
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
