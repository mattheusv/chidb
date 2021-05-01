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

use pager::{MemPage, Pager, HEADER_SIZE, PAGE_SIZE};

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
            btree.initialize_empty_table_leaf()?;
            Ok(btree)
        } else {
            let mut btree = BTree { pager };
            btree.validate_header()?;
            Ok(btree)
        }
    }

    /// Loads a B-Tree node from disk
    ///
    /// Reads a B-Tree node from a page in the disk. All the information regarding
    /// the node is stored in a BTreeNode struct (see header file for more details
    /// on this struct).
    /// Any changes made to a BTreeNode variable will not be effective in the database
    /// until write_node is called on that BTreeNode.
    ///
    /// Parameters
    /// - n_page: Page of node to load
    pub fn get_node_by_page(&mut self, n_page: u32) -> Result<BTreeNode, ChiError> {
        let page = self.pager.read_page(n_page)?;
        BTreeNode::load_from_page(page)
    }

    /// Create a new B-Tree node
    ///
    /// Allocates a new page in the file and initializes it as an empty B-Tree node.
    ///
    /// Parameters
    /// - type: Type of B-Tree node
    pub fn new_node(&mut self, typ: BTreeNodeType) -> Result<BTreeNode, ChiError> {
        let n_page = self.pager.allocate_page();
        let page = self.pager.read_page(n_page)?;

        let node = BTreeNode::create(page, typ)?;
        self.pager.write_page(&node.page)?;

        Ok(node)
    }

    /// Write an in-memory B-Tree node to disk
    ///
    /// Writes an in-memory B-Tree node to disk. To do this, we need to update
    /// the in-memory page according to the chidb page format. Since the cell
    /// offset array and the cells themselves are modified directly on the
    /// page, the only thing to do is to store the values of "type",
    /// "free_offset", "n_cells", "cells_offset" and "right_page" in the
    /// in-memory page.
    ///
    /// Parameters
    /// - node: BTreeNode to write to disk
    pub fn write_node(&mut self, node: &mut BTreeNode) -> Result<(), ChiError> {
        let page_data = node.page.data();
        let bytes = BytesMut::with_capacity(page_data.len());
        let mut buffer = BufWriter::with_capacity(page_data.len(), bytes.writer());

        buffer.write(&[node.typ.value()])?;
        buffer.write(&node.free_offset.to_le_bytes())?;
        buffer.write(&node.n_cells.to_le_bytes())?;
        buffer.write(&node.cells_offset.to_le_bytes())?;
        buffer.write(&node.right_page.to_le_bytes())?;

        node.page.set_data(buffer.buffer());
        self.pager.write_page(&node.page)?;

        Ok(())
    }

    fn validate_header(&mut self) -> Result<(), ChiError> {
        let header = self.read_header()?;
        if MAGIC_BYTES.clone() != header.magic_bytes {
            Err(ChiError::Ecorruptheader)
        } else {
            Ok(())
        }
    }

    fn initialize_empty_table_leaf(&mut self) -> Result<(), ChiError> {
        let n_page = self.pager.allocate_page();
        let page = self.pager.read_page(n_page)?;

        let node = BTreeNode::create(page, BTreeNodeType::LeafTable)?;
        self.pager.write_page(&node.page)?;
        Ok(())
    }

    fn initialize_header(&mut self) -> Result<(), ChiError> {
        let header = BTreeHeader::default();
        let mut header_bytes = self.pager.read_header()?;

        let raw = &mut header_bytes[0..HEADER_SIZE];
        raw.copy_from_slice(&header.to_bytes()?);

        // TODO: Fix this unecessary copy
        let mut copy = [0; HEADER_SIZE];
        copy.copy_from_slice(&raw[..]);
        self.pager.write_header(&copy)?;

        Ok(())
    }

    fn read_header(&mut self) -> Result<BTreeHeader, ChiError> {
        let header_bytes = self.pager.read_header()?;
        let header = BTreeHeader::from_bytes(&header_bytes)?;
        Ok(header)
    }
}

#[derive(Debug, PartialEq)]
pub enum BTreeNodeType {
    InternalTable,
    LeafTable,
    InternalIndex,
    LeafIndex,
}

impl From<[u8; 1]> for BTreeNodeType {
    fn from(typ: [u8; 1]) -> Self {
        match typ[0] {
            0x05 => Self::InternalTable,
            0x0D => Self::LeafTable,
            0x02 => Self::InternalIndex,
            0x0A => Self::LeafIndex,
            _ => panic!("Invalid type: {:?}", typ),
        }
    }
}

impl BTreeNodeType {
    fn value(&self) -> u8 {
        match self {
            Self::InternalTable => 0x05,
            Self::LeafTable => 0x0D,
            Self::InternalIndex => 0x02,
            Self::LeafIndex => 0x0A,
        }
    }
}

/// The BTreeNode struct is an in-memory representation of a B-Tree node. Thus,
/// most of the values in this struct are simply a copy, for ease of access,
/// of what can be found in the raw disk page. When modifying type, free_offset,
/// n_cells, cells_offset, or right_page, do so in the corresponding field
/// of the BTreeNode variable (the changes will be effective once the BTreeNode
/// is written to disk). Modifications of the
/// cell offset array or of the cells should be done directly on the in-memory
/// page returned by the Pager.
///
/// See The chidb File Format document for more details on the meaning of each
/// field.
pub struct BTreeNode {
    /// In-memory page returned by the Pager
    page: MemPage,

    /// The type of page
    typ: BTreeNodeType,

    /// The byte offset at which the free space starts.
    /// Note that this must be updated every time the cell offset array grows.
    free_offset: u16,

    /// The number of cells stored in this page.
    n_cells: u16,

    /// The byte offset at which the cells start. If the page contains no cells, this field contains the value PageSize.
    /// This value must be updated every time a cell is added.
    cells_offset: u16,

    /// Right page (internal nodes only)
    right_page: u16,

    /// Pointer to start of cell offset array in the in-memory page
    celloffset_array: u8,
}

impl BTreeNode {
    pub fn new(page: MemPage, typ: BTreeNodeType) -> Self {
        BTreeNode {
            page,
            typ,
            free_offset: 0,
            n_cells: 0,
            cells_offset: PAGE_SIZE as u16,
            right_page: 0,
            celloffset_array: 0,
        }
    }

    /// Create a BTreeNode from a empty MemPage
    ///
    /// This function create a new BTreeNode from a empty MemPage
    /// and populate the MemPage with the initial values of BTreeNode
    pub fn create(page: MemPage, typ: BTreeNodeType) -> Result<Self, ChiError> {
        let mut node = Self::new(page, typ);

        let page_data = node.page.data_as_mut();
        let page_len = page_data.len();

        let bytes = BytesMut::with_capacity(page_len);
        let mut buffer = BufWriter::with_capacity(page_len, bytes.writer());

        let mut bytes_writen = buffer.write(&[node.typ.value()])?;
        bytes_writen += buffer.write(&node.free_offset.to_le_bytes())?;
        bytes_writen += buffer.write(&node.n_cells.to_le_bytes())?;
        bytes_writen += buffer.write(&node.cells_offset.to_le_bytes())?;
        bytes_writen += buffer.write(&node.right_page.to_le_bytes())?;
        bytes_writen += buffer.write(&node.celloffset_array.to_le_bytes())?;

        let empty_space = vec![0; page_len - bytes_writen];
        buffer.write(&empty_space)?;

        page_data.copy_from_slice(buffer.buffer());

        Ok(node)
    }

    /// Load BTreeNode from a existing MemPage
    pub fn load_from_page(page: MemPage) -> Result<Self, ChiError> {
        let page_data = page.data();
        let mut buffer = BufReader::new(&page_data[..]);

        let mut typ: [u8; 1] = [0; 1];
        let mut free_offset = [0; size_of::<u16>()];
        let mut n_cells = [0; size_of::<u16>()];
        let mut cells_offset = [0; size_of::<u16>()];
        let mut righ_page = [0; size_of::<u16>()];
        let mut celloffset_array = [0; size_of::<u8>()];

        buffer.read(&mut typ)?;
        buffer.read(&mut free_offset)?;
        buffer.read(&mut n_cells)?;
        buffer.read(&mut cells_offset)?;
        buffer.read(&mut righ_page)?;
        buffer.read(&mut celloffset_array)?;

        Ok(BTreeNode {
            page,
            typ: BTreeNodeType::from(typ),
            free_offset: u16::from_le_bytes(free_offset),
            n_cells: u16::from_le_bytes(n_cells),
            cells_offset: u16::from_le_bytes(cells_offset),
            right_page: u16::from_le_bytes(righ_page),
            celloffset_array: u8::from_le_bytes(celloffset_array),
        })
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

    fn from_bytes(bytes: &[u8]) -> io::Result<BTreeHeader> {
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
    fn test_write_first_node_not_override_file_header() -> Result<(), ChiError> {
        let file = TempDir::new()?.into_path().join("test_write_node");

        let mut btree = BTree::open(&file)?;
        let mut node = btree.get_node_by_page(1)?;
        node.free_offset += 1;
        btree.write_node(&mut node)?;

        let updated_node = btree.get_node_by_page(node.page.n_page)?;
        assert_eq!(
            updated_node.free_offset, node.free_offset,
            "Expected values updated after write first node"
        );

        Ok(())
    }

    #[test]
    fn test_write_node() -> Result<(), ChiError> {
        let file = TempDir::new()?.into_path().join("test_write_node");

        let mut btree = BTree::open(&file)?;
        let mut node = btree.new_node(BTreeNodeType::InternalTable)?;
        node.free_offset += 1;
        btree.write_node(&mut node)?;

        let updated_node = btree.get_node_by_page(node.page.n_page)?;
        assert_eq!(
            updated_node.free_offset, node.free_offset,
            "Expected values updated after write node"
        );

        Ok(())
    }

    #[test]
    fn test_create_new_node() -> Result<(), ChiError> {
        let file = TempDir::new()?.into_path().join("test_create_new_node");

        let mut btree = BTree::open(&file)?;
        let node = btree.new_node(BTreeNodeType::InternalTable)?;

        assert_eq!(node.page.n_page, 2);
        assert_eq!(node.typ, BTreeNodeType::InternalTable);
        assert_eq!(node.free_offset, 0);
        assert_eq!(node.n_cells, 0);
        assert_eq!(node.cells_offset, PAGE_SIZE as u16);
        assert_eq!(node.right_page, 0);
        assert_eq!(node.celloffset_array, PAGE_HEADER_SIZE);

        // Assert that we can read the node correctly
        let new_node = btree.get_node_by_page(node.page.n_page)?;
        assert_eq!(
            node.page.n_page, new_node.page.n_page,
            "Expected equals n_page"
        );
        assert_eq!(node.typ, new_node.typ, "Expect equal types");
        assert_eq!(
            node.free_offset, new_node.free_offset,
            "Expected equal free_offset"
        );
        assert_eq!(node.n_cells, new_node.n_cells, "Expetec equals n_cells");
        assert_eq!(
            node.cells_offset, new_node.cells_offset,
            "Expected equal cells_offset"
        );
        assert_eq!(
            node.right_page, new_node.right_page,
            "Expected equal righ_page"
        );
        assert_eq!(
            node.celloffset_array, new_node.celloffset_array,
            "Expected equal celloffset_array"
        );

        Ok(())
    }

    #[test]
    fn test_first_node_page_leaf_table() -> Result<(), ChiError> {
        let file = TempDir::new()?.into_path().join("test_create_new_node");

        let mut btree = BTree::open(&file)?;

        let node = btree.get_node_by_page(1)?;

        assert_eq!(node.page.n_page, 1);
        assert_eq!(node.typ, BTreeNodeType::LeafTable);

        Ok(())
    }

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
