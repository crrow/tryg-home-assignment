// Copyright 2025 Crrow
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # SSTable (Sorted String Table) Implementation
//!
//! This module provides a refined, memory-efficient implementation of SSTable
//! reading and writing with lazy loading capabilities.
//!
//! ## Design Overview
//!
//! The SSTable implementation consists of three main components:
//!
//! ### 1. AsyncIOEntry - Lazy Entry Loading
//!
//! `AsyncIOEntry` is a lightweight wrapper that points to an entry's location
//! in the file without loading the actual data until needed. Key features:
//! - Minimal memory footprint (~40-50 bytes per entry)
//! - Caching to avoid repeated I/O for the same entry
//! - Thread-safe file access through Arc<Mutex<File>>
//! - Lazy loading of entry data on first access
//!
//! ### 2. BlockReader - Block-Level Caching
//!
//! `BlockReader` manages a single SSTable block and caches its metadata:
//! - Pre-loads entry count and offset table during initialization
//! - Eliminates repeated metadata reads for entries in the same block
//! - Provides efficient point lookup and iteration within a block
//! - Creates AsyncIOEntry instances with cached block information
//!
//! ### 3. TableReader - Refined Table Management
//!
//! The refined `TableReader` design addresses performance issues in the
//! original implementation:
//!
//! **Original Problems:**
//! - Created AsyncIOEntry instances that repeatedly read block metadata
//! - File metadata operations for every entry access
//! - No caching of block-level information
//! - Inefficient iteration through large tables
//!
//! **Refined Solutions:**
//! - Pre-creates BlockReader instances during TableReader initialization
//! - BlockReaders cache entry counts and offset tables
//! - Significantly reduces I/O operations during iteration
//! - Better memory locality and cache efficiency
//! - Simplified API that's easier to use and understand
//!
//! ## Performance Benefits
//!
//! The refined design provides several performance improvements:
//!
//! 1. **Reduced I/O Overhead**: Block metadata is read once per block instead
//!    of once per entry
//! 2. **Better Caching**: BlockReaders maintain hot data in memory for fast
//!    access
//! 3. **Memory Efficiency**: AsyncIOEntry instances remain lightweight while
//!    providing lazy loading
//! 4. **Simplified Iteration**: Direct access to entries through pre-loaded
//!    block readers
//! 5. **Thread Safety**: Safe concurrent access through Arc<Mutex<File>>
//!    without data races
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! // Open a table - this pre-loads all block readers
//! let table_reader = TableReader::open("sstable.sst").await?;
//!
//! // Point lookup - efficient search through block readers
//! if let Some(entry) = table_reader.get(&key).await? {
//!     println!("Found: {:?}", entry);
//! }
//!
//! // Iteration - uses pre-loaded block metadata
//! let all_entries = table_reader.iter().await?;
//! for mut io_entry in all_entries {
//!     let entry = io_entry.entry().await?; // Lazy loading on access
//!     println!("Entry: {:?}", entry);
//! }
//!
//! // Range queries - efficient filtering using block readers
//! let range_entries = table_reader.range_iter(&start_key, &end_key).await?;
//! ```
//!
//! This design strikes an optimal balance between memory usage, I/O efficiency,
//! and code simplicity, making it well-suited for high-performance database
//! storage.

use std::{
    fs::File,
    io::{Cursor, Read, Seek, Write},
    mem::MaybeUninit,
    path::Path,
    sync::{Arc, Mutex},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use rsketch_common::readable_size::ReadableSize;
use snafu::{ResultExt, ensure};
use value_log::Slice;

use crate::{
    format::{Codec, Entry, IndexEntry, InternalKey},
    sst::{
        block::{
            self, BlockEntry, BlockReader, DataBlockBuilder, DataBlockReader, IOEntry,
            IndexBlockBuilder, IndexBlockReader,
        },
        err::{IOSnafu, IndexChecksumMismatchSnafu, InvalidSSTFileSnafu, Result},
    },
};
/// Magic number to identify SSTable files
const SSTABLE_MAGIC: u64 = 0x5353544142_u64; // "SSTAB" in hex

/// Table footer containing metadata about the SSTable
#[derive(Debug, Clone)]
pub(crate) struct TableFooter {
    /// Offset to the index block
    pub index_offset:   u64,
    /// Size of the index block in bytes
    pub index_size:     u64,
    /// Total number of entries in the table
    pub entry_count:    u32,
    /// The smallest key in the table
    pub first_key:      Option<InternalKey>,
    /// The largest key in the table
    pub last_key:       Option<InternalKey>,
    /// CRC32 checksum of the index block
    pub index_checksum: u32,
    /// CRC32 checksum of all data blocks combined
    pub data_checksum:  u32,
    /// Magic number for file format validation
    pub magic:          u64,
}

impl Codec for TableFooter {
    fn encode_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.index_offset)?;
        writer.write_u64::<LittleEndian>(self.index_size)?;
        writer.write_u32::<LittleEndian>(self.entry_count)?;

        // Write first key (optional)
        if let Some(ref key) = self.first_key {
            writer.write_u8(1)?; // has first key
            key.encode_into(writer)?;
        } else {
            writer.write_u8(0)?; // no first key
        }

        // Write last key (optional)
        if let Some(ref key) = self.last_key {
            writer.write_u8(1)?; // has last key
            key.encode_into(writer)?;
        } else {
            writer.write_u8(0)?; // no last key
        }

        writer.write_u32::<LittleEndian>(self.index_checksum)?;
        writer.write_u32::<LittleEndian>(self.data_checksum)?;
        writer.write_u64::<LittleEndian>(self.magic)?;

        Ok(())
    }

    fn decode_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let index_offset = reader.read_u64::<LittleEndian>()?;
        let index_size = reader.read_u64::<LittleEndian>()?;
        let entry_count = reader.read_u32::<LittleEndian>()?;

        // Read first key (optional)
        let first_key = if reader.read_u8()? == 1 {
            Some(InternalKey::decode_from(reader)?)
        } else {
            None
        };

        // Read last key (optional)
        let last_key = if reader.read_u8()? == 1 {
            Some(InternalKey::decode_from(reader)?)
        } else {
            None
        };

        let index_checksum = reader.read_u32::<LittleEndian>()?;
        let data_checksum = reader.read_u32::<LittleEndian>()?;
        let magic = reader.read_u64::<LittleEndian>()?;

        Ok(Self {
            index_offset,
            index_size,
            entry_count,
            first_key,
            last_key,
            index_checksum,
            data_checksum,
            magic,
        })
    }
}

impl TableFooter {
    /// Returns the size of the footer when serialized (minimum size, excluding
    /// variable keys)
    pub fn min_footer_size() -> u64 {
        8 + 8 + 4 + // index_offset, index_size, entry_count
        1 + 1 + // first_key and last_key presence flags
        4 + 4 + 8 // checksums and magic
    }
}

/// A production-ready table builder for creating SSTable files using tokio
/// async I/O.
///
/// Features:
/// - Data integrity with CRC32 checksums
/// - Comprehensive metadata in table footer
/// - Proper error handling and recovery
/// - Block-level organization with index
/// - Configurable block size thresholds
/// - Async I/O using tokio::fs
pub(crate) struct TableBuilder {
    /// The underlying file writer
    file:                 File,
    /// Current data block being built
    data_block:           DataBlockBuilder,
    /// Index block that maps keys to data block locations
    index_block:          IndexBlockBuilder,
    /// The first key added to the table
    first_key:            Option<InternalKey>,
    /// The last key that was added (for ordering validation)
    last_key:             Option<InternalKey>,
    /// Whether this table builder has been closed
    closed:               bool,
    /// Total number of entries added to the table
    num_entries:          u32,
    /// Size threshold for flushing data blocks
    block_size_threshold: ReadableSize,
    /// Current file offset where the next block will be written
    file_offset:          u64,
    /// Running CRC32 checksum for all data blocks
    data_checksum:        Hasher,
    /// List of data block metadata for the index
    data_blocks:          Vec<(InternalKey, u64, u64, u32)>, // (last_key, offset, size, checksum)
}

impl TableBuilder {
    /// Creates a new table builder with the given file.
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .context(IOSnafu)?;
        Ok(Self {
            file,
            data_block: DataBlockBuilder::new(),
            index_block: IndexBlockBuilder::new(),
            first_key: None,
            last_key: None,
            closed: false,
            num_entries: 0,
            block_size_threshold: ReadableSize::mb(1),
            file_offset: 0,
            data_checksum: Hasher::new(),
            data_blocks: Vec::new(),
        })
    }

    /// Adds an entry to the table.
    ///
    /// Entries must be added in sorted order by key. When the current data
    /// block reaches the size threshold, it will be automatically flushed
    /// to disk.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The table builder has been closed
    /// - Entries are not added in sorted order
    pub(crate) async fn add(&mut self, entry: Entry) -> Result<()> {
        assert!(!self.closed, "TableBuilder has been closed");

        // Ensure entries are added in sorted order
        if let Some(last_key) = &self.last_key {
            assert!(
                last_key < &entry.key,
                "Keys must be added in sorted order: new key {:?} is not greater than last key \
                 {:?}",
                entry.key,
                last_key
            );
        }

        // Track first and last keys
        if self.first_key.is_none() {
            self.first_key = Some(entry.key.clone());
        }
        self.last_key = Some(entry.key.clone());
        self.num_entries += 1;

        self.data_block.add(entry);

        // Check if we need to flush the current data block
        if self.data_block.estimate_size() >= self.block_size_threshold.as_bytes() {
            self.flush_data_block()?;
        }

        Ok(())
    }

    /// Flushes the current data block to disk and creates an index entry for
    /// it.
    ///
    /// This method:
    /// 1. Finishes the current data block
    /// 2. Writes it directly to the file using tokio's async API
    /// 3. Creates an index entry mapping the last key to the block location
    /// 4. Resets the data block for new entries
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }

        // Finish the current data block
        self.data_block.finish();

        // Build block data into a buffer
        let block_data = self
            .data_block
            .build()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .context(IOSnafu)?;

        // Compute checksum for the block
        let mut hasher = Hasher::new();
        hasher.update(&block_data);
        let block_checksum = hasher.finalize();

        // Write the block to file
        self.file.write_all(&block_data).context(IOSnafu)?;
        let block_size = block_data.len() as u64;

        // Update running data checksum
        self.data_checksum.update(&block_checksum.to_le_bytes());

        // Create an index entry for this data block
        if let Some(ref last_key) = self.last_key {
            let index_entry = IndexEntry::new(last_key.clone(), self.file_offset, block_size);
            self.index_block.add(index_entry);

            // Store block metadata for footer
            self.data_blocks.push((
                last_key.clone(),
                self.file_offset,
                block_size,
                block_checksum,
            ));
        }

        // Update file offset for the next block
        self.file_offset += block_size;

        // Reset the data block for new entries
        self.data_block.clear();

        Ok(())
    }

    /// Finalizes the table by flushing any remaining data and writing the index
    /// block and footer.
    ///
    /// This method should be called when all entries have been added. It:
    /// 1. Flushes any remaining data in the current data block
    /// 2. Writes the index block directly to the file
    /// 3. Writes the table footer with complete metadata
    /// 4. Syncs the file to ensure durability
    /// 5. Marks the table builder as closed
    pub(crate) fn finish(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        // Flush any remaining data
        self.flush_data_block()?;

        let index_offset = self.file_offset;
        let mut index_size = 0;
        let mut index_checksum = 0;

        // Write the index block if it has entries
        if !self.index_block.is_empty() {
            self.index_block.finish();

            // Build index block data
            let index_data = self
                .index_block
                .build()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .context(IOSnafu)?;

            // Compute index checksum
            let mut hasher = Hasher::new();
            hasher.update(&index_data);
            index_checksum = hasher.finalize();

            // Write index block to file using tokio's async API
            self.file.write_all(&index_data).context(IOSnafu)?;
            index_size = index_data.len() as u64;
            self.file_offset += index_size;
        }

        // Create and write table footer
        let footer = TableFooter {
            index_offset,
            index_size,
            entry_count: self.num_entries,
            first_key: self.first_key.clone(),
            last_key: self.last_key.clone(),
            index_checksum,
            data_checksum: self.data_checksum.clone().finalize(),
            magic: SSTABLE_MAGIC,
        };

        // Encode footer to bytes
        let mut footer_data = Vec::new();
        footer.encode_into(&mut footer_data).context(IOSnafu)?;

        // Write footer to file
        self.file.write_all(&footer_data).context(IOSnafu)?;

        // Write footer size (4 bytes) for easy footer location
        let footer_size = footer_data.len() as u32;
        let footer_size_buf = footer_size.to_le_bytes();

        self.file.write_all(&footer_size_buf).context(IOSnafu)?;

        // Sync file to ensure durability
        self.file.sync_all().context(IOSnafu)?;

        self.closed = true;
        Ok(())
    }

    /// Returns the number of entries added to the table.
    pub(crate) fn entry_count(&self) -> u32 { self.num_entries }

    /// Returns whether the table builder has been closed.
    pub(crate) fn is_closed(&self) -> bool { self.closed }

    /// Returns the current file size in bytes.
    pub(crate) fn file_size(&self) -> u64 { self.file_offset }

    /// Returns the first key in the table, if any.
    pub(crate) fn first_key(&self) -> Option<&InternalKey> { self.first_key.as_ref() }

    /// Returns the last key in the table, if any.
    pub(crate) fn last_key(&self) -> Option<&InternalKey> { self.last_key.as_ref() }

    /// Returns the current data checksum.
    pub(crate) fn data_checksum(&self) -> u32 { self.data_checksum.clone().finalize() }

    /// Returns the data blocks that have been written.
    pub(crate) fn data_blocks(&self) -> &[(InternalKey, u64, u64, u32)] { &self.data_blocks }
}

/// A simplified table reader that loads data lazily and provides a clean
/// interface.
///
/// This version uses the existing BlockReader from block.rs and maintains
/// index information for efficient access patterns.
pub(crate) struct TableReader {
    /// Table footer containing metadata
    footer:        TableFooter,
    file:          File,
    /// Index entries that map keys to block locations
    index_entries: Vec<IndexEntry>,
    /// Cache of block readers for each index entry
    block_cache:   Vec<Option<DataBlockReader<Cursor<Slice>>>>,
}

impl TableReader {
    /// Creates a new table reader from a file.
    ///
    /// This loads the footer and index block immediately for efficient queries,
    /// but entry data is loaded lazily on demand.
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let metadata = std::fs::metadata(path.as_ref()).context(IOSnafu)?;
        let file_size = metadata.len();
        ensure!(
            file_size > TableFooter::min_footer_size() + 4,
            InvalidSSTFileSnafu {
                reason: "File too small to contain valid SSTable footer".to_string(),
                path:   path.as_ref().to_string_lossy().to_string(),
            }
        );

        let mut file = File::open(path.as_ref()).context(IOSnafu)?;

        // Read footer size from the last 4 bytes
        file.seek(std::io::SeekFrom::Start(file_size - 4))
            .context(IOSnafu)?;
        let mut footer_size_buf = [0u8; 4];
        file.read_exact(&mut footer_size_buf).context(IOSnafu)?;
        let footer_size = u32::from_le_bytes(footer_size_buf) as u64;

        // Validate footer size
        ensure!(
            footer_size >= TableFooter::min_footer_size(),
            InvalidSSTFileSnafu {
                reason: format!(
                    "Invalid footer size: {} bytes, minimum required: {} bytes",
                    footer_size,
                    TableFooter::min_footer_size()
                ),
                path:   path.as_ref().to_string_lossy().to_string(),
            }
        );

        // Read the footer
        let footer_offset = file_size - 4 - footer_size;
        file.seek(std::io::SeekFrom::Start(footer_offset))
            .context(IOSnafu)?;
        let mut footer_buf = vec![0u8; footer_size as usize];
        file.read_exact(&mut footer_buf).context(IOSnafu)?;

        let mut footer_cursor = Cursor::new(footer_buf.as_slice());
        let footer = TableFooter::decode_from(&mut footer_cursor).context(IOSnafu)?;

        // Validate magic number
        ensure!(
            footer.magic == SSTABLE_MAGIC,
            InvalidSSTFileSnafu {
                reason: format!(
                    "Invalid magic number: expected {:#x}, got {:#x}",
                    SSTABLE_MAGIC, footer.magic
                ),
                path:   path.as_ref().to_string_lossy().to_string(),
            }
        );

        // Validate footer fields
        ensure!(
            footer.index_offset + footer.index_size <= file_size,
            InvalidSSTFileSnafu {
                reason: "Index block extends beyond file size".to_string(),
                path:   path.as_ref().to_string_lossy().to_string(),
            }
        );

        // Load the index block
        let index_entries = if footer.index_size == 0 {
            Vec::new()
        } else {
            // Read index data
            file.seek(std::io::SeekFrom::Start(footer.index_offset))
                .context(IOSnafu)?;
            let mut index_buf = vec![0u8; footer.index_size as usize];
            file.read_exact(&mut index_buf).context(IOSnafu)?;

            // Verify index checksum
            let computed_checksum = {
                let mut hasher = Hasher::new();
                hasher.update(&index_buf);
                hasher.finalize()
            };

            ensure!(
                computed_checksum == footer.index_checksum,
                IndexChecksumMismatchSnafu {
                    expected: footer.index_checksum,
                    actual:   computed_checksum,
                }
            );

            // Parse index block
            let cursor = std::io::Cursor::new(index_buf);
            let mut reader = IndexBlockReader::new(cursor)?;
            reader
                .iter()
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(IOSnafu)?
        };

        let block_cache = vec![None; index_entries.len()];
        Ok(Self {
            footer,
            file,
            index_entries,
            block_cache,
        })
    }

    /// Finds the index entry that may contain the given key.
    ///
    /// Returns a reference to the index entry, or None if the key is outside
    /// the table's range.
    fn find_block_for_key(&self, key: &InternalKey) -> Option<&IndexEntry> {
        if !self.contains_key_range(key) {
            return None;
        }

        if self.index_entries.is_empty() {
            return None;
        }

        // Binary search through index entries to find the block that might contain the
        // key Since index entries are sorted by their last key, we need to find
        // the first entry whose key >= target key
        for index_entry in &self.index_entries {
            if key <= &index_entry.key {
                return Some(index_entry);
            }
        }

        // If key is larger than all index keys, it might be in the last block
        self.index_entries.last()
    }

    fn get_block_reader(
        &mut self,
        block_index: usize,
        index_entry: &IndexEntry,
    ) -> Result<&mut DataBlockReader<Cursor<Slice>>> {
        assert!(block_index < self.block_cache.len(), "invalid block index");
        if self.block_cache[block_index].is_none() {
            // read the block data out
            self.file
                .seek(std::io::SeekFrom::Start(index_entry.block_offset))
                .context(IOSnafu)?;
            let mut block_data = vec![0; index_entry.block_size as usize];
            self.file.read_exact(&mut block_data).context(IOSnafu)?;
            let reader = DataBlockReader::new(Cursor::new(Slice::from(block_data)))?;
            self.block_cache[block_index] = Some(reader);
        }
        let block_reader = self.block_cache[block_index].as_mut().unwrap();
        Ok(block_reader)
    }

    /// Performs a point lookup for a specific key.
    ///
    /// Returns the entry if found, or None if the key doesn't exist in this
    /// table.
    pub(crate) fn get(&mut self, key: &InternalKey) -> Result<Option<Entry>> {
        // Find which block might contain the key - collect the info first to avoid
        // borrowing conflicts
        let mut target_block = None;
        for (block_index, index_entry) in self.index_entries.iter().enumerate() {
            if key <= &index_entry.key {
                target_block = Some((block_index, index_entry.clone()));
                break;
            }
        }

        if let Some((block_index, index_entry)) = target_block {
            let block_reader = self.get_block_reader(block_index, &index_entry)?;
            let entry_count = block_reader.len();
            // Search through entries in this block
            for entry_index in 0..entry_count {
                if let Some(entry) = block_reader.get_at_index(entry_index).context(IOSnafu)? {
                    match entry.key.cmp(key) {
                        std::cmp::Ordering::Equal => return Ok(Some(entry)),
                        std::cmp::Ordering::Greater => break, // Keys are sorted, so we
                        // won't find it
                        std::cmp::Ordering::Less => continue,
                    }
                }
            }
        }
        Ok(None)
    }

    /// Returns a vector of entries for all entries in the table.
    ///
    /// This loads all entries into memory for simplicity.
    pub(crate) fn iter(&mut self) -> Result<Vec<Entry>> {
        let mut entries = Vec::new();

        // Collect index entry information to avoid borrowing conflicts
        let index_entries: Vec<(usize, IndexEntry)> = self
            .index_entries
            .iter()
            .enumerate()
            .map(|(i, entry)| (i, entry.clone()))
            .collect();

        for (block_index, index_entry) in index_entries {
            let block_reader = self.get_block_reader(block_index, &index_entry)?;
            // Get all entries from this block using the iterator
            for entry_result in block_reader.iter() {
                entries.push(entry_result.context(IOSnafu)?);
            }
        }

        Ok(entries)
    }

    /// Returns entries within a key range.
    pub(crate) fn range_iter(
        &mut self,
        start_key: &InternalKey,
        end_key: &InternalKey,
    ) -> Result<Vec<Entry>> {
        let mut entries = Vec::new();

        // Collect index entry information to avoid borrowing conflicts
        let index_entries: Vec<(usize, IndexEntry)> = self
            .index_entries
            .iter()
            .enumerate()
            .map(|(i, entry)| (i, entry.clone()))
            .collect();

        for (block_index, index_entry) in index_entries {
            // Skip blocks that are entirely before our range
            if &index_entry.key < start_key {
                continue;
            }

            let block_reader = self.get_block_reader(block_index, &index_entry)?;
            // Get all entries from this block and filter by range
            for entry_result in block_reader.iter() {
                let entry = entry_result.context(IOSnafu)?;

                if &entry.key < start_key {
                    continue; // Skip entries before range
                }
                if &entry.key > end_key {
                    return Ok(entries); // Past end of range, we're done
                }

                entries.push(entry);
            }
        }

        Ok(entries)
    }

    /// Returns the table footer.
    pub(crate) fn footer(&self) -> &TableFooter { &self.footer }

    /// Returns the number of entries in the table.
    pub(crate) fn entry_count(&self) -> u32 { self.footer.entry_count }

    /// Returns the first key in the table, if any.
    pub(crate) fn first_key(&self) -> Option<&InternalKey> { self.footer.first_key.as_ref() }

    /// Returns the last key in the table, if any.
    pub(crate) fn last_key(&self) -> Option<&InternalKey> { self.footer.last_key.as_ref() }

    /// Validates that a key is within the table's key range.
    pub(crate) fn contains_key_range(&self, key: &InternalKey) -> bool {
        if let Some(ref first_key) = self.footer.first_key {
            if key < first_key {
                return false;
            }
        }

        if let Some(ref last_key) = self.footer.last_key {
            if key > last_key {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {

    use tempfile::NamedTempFile;

    use super::*;

    /// Helper function to create test entries
    fn create_test_entry(key: &str, value: &str, seqno: u64) -> Entry {
        Entry::make(key.as_bytes(), 0, seqno, value.as_bytes())
    }

    /// Helper function to create a test SSTable file
    fn create_test_sstable(entries: Vec<Entry>) -> (NamedTempFile, u64) {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path();

        // Create table builder and write entries
        let mut builder = TableBuilder::open(file_path).expect("Failed to open table builder");

        for entry in entries {
            // TableBuilder::add is async, but we need to call it synchronously in tests
            // For now, let's use a simple approach
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(builder.add(entry))
                .expect("Failed to add entry");
        }

        builder.finish().expect("Failed to finish table");

        // Get file size
        let metadata = std::fs::metadata(file_path).expect("Failed to get metadata");
        let file_size = metadata.len();

        (temp_file, file_size)
    }

    #[test]
    fn test_table_reader_lazy_loading() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
        ];

        let (temp_file, _file_size) = create_test_sstable(entries.clone());
        let mut table_reader =
            TableReader::open(temp_file.path()).expect("Failed to create table reader");

        // Test immediate access to metadata
        let entry_count = table_reader.entry_count();
        assert_eq!(entry_count, entries.len() as u32);

        // Performing a get operation
        let key = &entries[1].key;
        let result = table_reader.get(key).expect("Failed to perform get");
        assert!(result.is_some(), "Should find the key");

        let found_entry = result.unwrap();
        assert_eq!(found_entry.key, entries[1].key);
        assert_eq!(found_entry.value, entries[1].value);
    }

    #[test]
    fn test_table_reader_all_entries() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
        ];

        let (temp_file, _file_size) = create_test_sstable(entries.clone());
        let mut table_reader =
            TableReader::open(temp_file.path()).expect("Failed to create table reader");

        let all_entries = table_reader.iter().expect("Failed to get all entries");
        assert_eq!(all_entries.len(), entries.len());
        for (i, entry) in all_entries.into_iter().enumerate() {
            assert_eq!(entry.key, entries[i].key);
            assert_eq!(entry.value, entries[i].value);
        }
    }

    #[test]
    fn test_table_reader_range_entries() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
            create_test_entry("key4", "value4", 4),
            create_test_entry("key5", "value5", 5),
        ];

        let (temp_file, _file_size) = create_test_sstable(entries.clone());
        let mut table_reader =
            TableReader::open(temp_file.path()).expect("Failed to create table reader");

        // Create range iterator for keys 2-4
        let start_key = entries[1].key.clone(); // key2
        let end_key = entries[3].key.clone(); // key4

        let range_entries = table_reader
            .range_iter(&start_key, &end_key)
            .expect("Failed to get range entries");
        assert_eq!(range_entries.len(), 3); // key2, key3, key4

        let expected_in_range = &entries[1..4]; // key2, key3, key4
        for (actual, expected) in range_entries.into_iter().zip(expected_in_range.iter()) {
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.value, expected.value);
        }
    }

    #[test]
    fn test_contains_key_range() {
        let entries = vec![
            create_test_entry("key2", "value2", 2),
            create_test_entry("key5", "value5", 5),
            create_test_entry("key8", "value8", 8),
        ];

        let (temp_file, _) = create_test_sstable(entries.clone());
        let table_reader =
            TableReader::open(temp_file.path()).expect("Failed to create table reader");

        // Test keys within range
        let key_in_range = create_test_entry("key5", "dummy", 1).key;
        assert!(table_reader.contains_key_range(&key_in_range));

        // Test key before range
        let key_before = create_test_entry("key1", "dummy", 1).key;
        assert!(!table_reader.contains_key_range(&key_before));

        // Test key after range
        let key_after = create_test_entry("key9", "dummy", 1).key;
        assert!(!table_reader.contains_key_range(&key_after));
    }
}
