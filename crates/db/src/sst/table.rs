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

use std::{io::Cursor, path::Path, sync::Arc};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use rsketch_common::readable_size::ReadableSize;
use snafu::{ResultExt, ensure};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::{
    format::{Codec, Entry, IndexEntry, InternalKey},
    sst::{
        block::{DataBlockBuilder, IndexBlockBuilder, IndexBlockReader},
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
    pub(crate) async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
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
            self.flush_data_block().await?;
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
    async fn flush_data_block(&mut self) -> Result<()> {
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

        // Write the block to file using tokio's async API
        self.file.write_all(&block_data).await.context(IOSnafu)?;
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
    pub(crate) async fn finish(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        // Flush any remaining data
        self.flush_data_block().await?;

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
            self.file.write_all(&index_data).await.context(IOSnafu)?;
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
        self.file.write_all(&footer_data).await.context(IOSnafu)?;

        // Write footer size (4 bytes) for easy footer location
        let footer_size = footer_data.len() as u32;
        let footer_size_buf = footer_size.to_le_bytes();

        self.file
            .write_all(&footer_size_buf)
            .await
            .context(IOSnafu)?;

        // Sync file to ensure durability
        self.file.sync_all().await.context(IOSnafu)?;

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

/// An entry that can lazily read its data from the underlying file.
///
/// This provides the same interface as Entry but only reads the actual
/// entry data when it's accessed, making iterations memory-efficient.
#[derive(Debug, Clone)]
pub(crate) struct IoEntry {
    /// Shared file handle for reading entry data
    file:         Arc<Mutex<File>>,
    /// Starting offset of the block containing this entry
    block_offset: u64,
    /// Size of the block containing this entry
    block_size:   u64,
    /// Index of this entry within the block
    entry_index:  usize,
    /// Cached entry data (loaded lazily)
    cached_entry: Option<Entry>,
}

impl IoEntry {
    /// Creates a new IoEntry.
    pub(crate) fn new(
        file: Arc<Mutex<File>>,
        block_offset: u64,
        block_size: u64,
        entry_index: usize,
    ) -> Self {
        Self {
            file,
            block_offset,
            block_size,
            entry_index,
            cached_entry: None,
        }
    }

    /// Lazily loads and returns the entry data.
    pub(crate) async fn entry(&mut self) -> Result<&Entry> {
        if self.cached_entry.is_none() {
            let entry = self.read_entry_data().await?;
            self.cached_entry = Some(entry);
        }
        Ok(self.cached_entry.as_ref().unwrap())
    }

    /// Gets the key without loading the full entry (if cached).
    /// If not cached, this will load the full entry.
    pub(crate) async fn key(&mut self) -> Result<&InternalKey> {
        let entry = self.entry().await?;
        Ok(&entry.key)
    }

    /// Gets the value without loading the full entry (if cached).
    /// If not cached, this will load the full entry.
    pub(crate) async fn value(&mut self) -> Result<&[u8]> {
        let entry = self.entry().await?;
        Ok(&entry.value)
    }

    /// Reads the entry data from the file.
    async fn read_entry_data(&self) -> Result<Entry> {
        let mut file = self.file.lock().await;

        // Read entry count from the last 4 bytes of the block
        file.seek(std::io::SeekFrom::Start(
            self.block_offset + self.block_size - 4,
        ))
        .await
        .context(IOSnafu)?;
        let mut count_buf = [0u8; 4];
        file.read_exact(&mut count_buf).await.context(IOSnafu)?;
        let entry_count = u32::from_le_bytes(count_buf);

        // Validate entry index
        if self.entry_index >= entry_count as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Entry index {} out of bounds, valid range: 0..{}",
                    self.entry_index, entry_count
                ),
            ))
            .context(IOSnafu);
        }

        // Read offset for the specific entry
        let offset_section_size = entry_count as u64 * 8; // 8 bytes per u64 offset
        let offset_table_start = self.block_offset + self.block_size - 4 - offset_section_size;
        let entry_offset_pos = offset_table_start + (self.entry_index as u64 * 8);

        file.seek(std::io::SeekFrom::Start(entry_offset_pos))
            .await
            .context(IOSnafu)?;
        let mut offset_buf = [0u8; 8];
        file.read_exact(&mut offset_buf).await.context(IOSnafu)?;
        let entry_offset = u64::from_le_bytes(offset_buf);

        // Read the entry data
        let absolute_entry_offset = self.block_offset + entry_offset;
        file.seek(std::io::SeekFrom::Start(absolute_entry_offset))
            .await
            .context(IOSnafu)?;

        // Read entry using buffered approach
        let mut buffer = Vec::with_capacity(1024);
        let mut temp_buf = [0u8; 1024];

        // Read initial chunk
        let bytes_read = file.read(&mut temp_buf).await.context(IOSnafu)?;
        buffer.extend_from_slice(&temp_buf[..bytes_read]);

        // Try to decode the entry
        let mut cursor = Cursor::new(&buffer);
        loop {
            match Entry::decode_from(&mut cursor) {
                Ok(entry) => return Ok(entry),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Need more data, read another chunk
                    let current_pos = cursor.position() as usize;
                    if current_pos == buffer.len() {
                        // We've consumed all available data, read more
                        let bytes_read = file.read(&mut temp_buf).await.context(IOSnafu)?;
                        if bytes_read == 0 {
                            // End of file reached
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Incomplete entry data",
                            ))
                            .context(IOSnafu);
                        }
                        buffer.extend_from_slice(&temp_buf[..bytes_read]);
                        cursor = Cursor::new(&buffer);
                        cursor.set_position(current_pos as u64);
                    } else {
                        return Err(e).context(IOSnafu);
                    }
                }
                Err(e) => return Err(e).context(IOSnafu),
            }
        }
    }
}

/// A simplified table reader that loads data lazily and provides a clean
/// interface.
///
/// This replaces the complex TableReader/LazyBlockReader separation with a
/// unified approach that loads the footer and index immediately, but loads
/// entry data lazily.
pub(crate) struct TableReader {
    /// The underlying file reader (wrapped in Arc<Mutex> for sharing with
    /// IoEntry)
    file:          Arc<Mutex<File>>,
    /// Table footer containing metadata
    footer:        TableFooter,
    /// Index block for efficient lookups
    index_entries: Vec<IndexEntry>,
    /// File size for validation
    file_size:     u64,
}

impl TableReader {
    /// Creates a new table reader from a file.
    ///
    /// This loads the footer and index block immediately for efficient queries,
    /// but entry data is loaded lazily on demand.
    pub(crate) async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let metadata = tokio::fs::metadata(path.as_ref()).await.context(IOSnafu)?;
        let file_size = metadata.len();
        ensure!(
            file_size > TableFooter::min_footer_size() + 4,
            InvalidSSTFileSnafu {
                reason: "File too small to contain valid SSTable footer".to_string(),
                path:   path.as_ref().to_string_lossy().to_string(),
            }
        );

        let mut file = File::open(path.as_ref()).await.context(IOSnafu)?;

        // Read footer size from the last 4 bytes
        file.seek(std::io::SeekFrom::Start(file_size - 4))
            .await
            .context(IOSnafu)?;
        let mut footer_size_buf = [0u8; 4];
        file.read_exact(&mut footer_size_buf)
            .await
            .context(IOSnafu)?;
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
            .await
            .context(IOSnafu)?;
        let mut footer_buf = vec![0u8; footer_size as usize];
        file.read_exact(&mut footer_buf).await.context(IOSnafu)?;

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
                .await
                .context(IOSnafu)?;
            let mut index_buf = vec![0u8; footer.index_size as usize];
            file.read_exact(&mut index_buf).await.context(IOSnafu)?;

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
            let mut reader = IndexBlockReader::new(cursor).context(IOSnafu)?;
            reader
                .iter()
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(IOSnafu)?
        };

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            footer,
            index_entries,
            file_size,
        })
    }

    /// Finds the data block that may contain the given key.
    ///
    /// Returns the block offset and size, or None if the key is outside the
    /// table's range.
    fn find_block_for_key(&self, key: &InternalKey) -> Option<(u64, u64)> {
        if !self.contains_key_range(key) {
            return None;
        }

        if self.index_entries.is_empty() {
            return None;
        }

        // Binary search through index entries to find the appropriate block
        match self
            .index_entries
            .binary_search_by(|entry| entry.key.cmp(key))
        {
            Ok(idx) => {
                // Exact match - the key is definitely in this block
                Some((
                    self.index_entries[idx].block_offset,
                    self.index_entries[idx].block_size,
                ))
            }
            Err(idx) => {
                // Key would be inserted at idx, so it might be in the block at idx
                if idx < self.index_entries.len() {
                    Some((
                        self.index_entries[idx].block_offset,
                        self.index_entries[idx].block_size,
                    ))
                } else {
                    // Key is larger than all index entries, check the last block
                    if let Some(last_entry) = self.index_entries.last() {
                        Some((last_entry.block_offset, last_entry.block_size))
                    } else {
                        None
                    }
                }
            }
        }
    }

    /// Gets the number of entries in a specific block.
    async fn get_block_entry_count(&self, block_offset: u64, block_size: u64) -> Result<u32> {
        let mut file = self.file.lock().await;
        // Read entry count from the last 4 bytes of the block
        file.seek(std::io::SeekFrom::Start(block_offset + block_size - 4))
            .await
            .context(IOSnafu)?;
        let mut count_buf = [0u8; 4];
        file.read_exact(&mut count_buf).await.context(IOSnafu)?;
        Ok(u32::from_le_bytes(count_buf))
    }

    /// Performs a point lookup for a specific key.
    ///
    /// Returns the entry if found, or None if the key doesn't exist in this
    /// table.
    pub(crate) async fn get(&mut self, key: &InternalKey) -> Result<Option<Entry>> {
        if let Some((offset, size)) = self.find_block_for_key(key) {
            let entry_count = self.get_block_entry_count(offset, size).await?;

            // Linear search through the block entries
            // TODO: Could be optimized with binary search within the block
            for entry_index in 0..entry_count as usize {
                let mut io_entry = IoEntry::new(Arc::clone(&self.file), offset, size, entry_index);
                let entry = io_entry.entry().await?;
                match entry.key.cmp(key) {
                    std::cmp::Ordering::Equal => return Ok(Some(entry.clone())),
                    std::cmp::Ordering::Greater => break, // Keys are sorted, so we won't find it
                    std::cmp::Ordering::Less => continue,
                }
            }
        }
        Ok(None)
    }

    /// Returns a lazy iterator that yields IoEntry instances for all entries in
    /// the table.
    ///
    /// This is much more memory-efficient than loading all entries into memory.
    pub(crate) async fn iter(&self) -> Result<Vec<IoEntry>> {
        let mut io_entries = Vec::new();

        for index_entry in &self.index_entries {
            let block_offset = index_entry.block_offset;
            let block_size = index_entry.block_size;
            let entry_count = self.get_block_entry_count(block_offset, block_size).await?;

            for entry_index in 0..entry_count as usize {
                let io_entry = IoEntry::new(
                    Arc::clone(&self.file),
                    block_offset,
                    block_size,
                    entry_index,
                );
                io_entries.push(io_entry);
            }
        }

        Ok(io_entries)
    }

    /// Returns IoEntry instances for entries within a key range.
    pub(crate) async fn range_iter(
        &self,
        start_key: &InternalKey,
        end_key: &InternalKey,
    ) -> Result<Vec<IoEntry>> {
        let mut io_entries = Vec::new();

        for index_entry in &self.index_entries {
            // Skip blocks that are entirely before our range
            if index_entry.key < *start_key {
                continue;
            }

            let block_offset = index_entry.block_offset;
            let block_size = index_entry.block_size;
            let entry_count = self.get_block_entry_count(block_offset, block_size).await?;

            for entry_index in 0..entry_count as usize {
                let mut io_entry = IoEntry::new(
                    Arc::clone(&self.file),
                    block_offset,
                    block_size,
                    entry_index,
                );

                // Check if this entry is within our range by examining its key
                let entry_key = io_entry.key().await?;
                if entry_key < start_key {
                    continue; // Skip entries before range
                }
                if entry_key > end_key {
                    return Ok(io_entries); // Past end of range, we're done
                }

                io_entries.push(io_entry);
            }
        }

        Ok(io_entries)
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
    async fn create_test_sstable(entries: Vec<Entry>) -> (NamedTempFile, u64) {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path();

        // Create table builder and write entries
        let mut builder = TableBuilder::open(file_path)
            .await
            .expect("Failed to open table builder");

        for entry in entries {
            builder.add(entry).await.expect("Failed to add entry");
        }

        builder.finish().await.expect("Failed to finish table");

        // Get file size
        let metadata = tokio::fs::metadata(file_path)
            .await
            .expect("Failed to get metadata");
        let file_size = metadata.len();

        (temp_file, file_size)
    }

    #[tokio::test]
    async fn test_table_reader_lazy_loading() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
        ];

        let (temp_file, _file_size) = create_test_sstable(entries.clone()).await;
        let mut table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        // Test immediate access to metadata
        let entry_count = table_reader.entry_count();
        assert_eq!(entry_count, entries.len() as u32);

        // Performing a get operation
        let key = &entries[1].key;
        let result = table_reader.get(key).await.expect("Failed to perform get");
        assert!(result.is_some(), "Should find the key");

        let found_entry = result.unwrap();
        assert_eq!(found_entry.key, entries[1].key);
        assert_eq!(found_entry.value, entries[1].value);
    }

    #[tokio::test]
    async fn test_table_reader_all_entries() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
        ];

        let (temp_file, _file_size) = create_test_sstable(entries.clone()).await;
        let table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        let all_entries = table_reader
            .iter()
            .await
            .expect("Failed to get all entries");
        assert_eq!(all_entries.len(), entries.len());
        for (i, mut io_entry) in all_entries.into_iter().enumerate() {
            let entry = io_entry.entry().await.expect("Failed to get entry");
            assert_eq!(entry.key, entries[i].key);
            assert_eq!(entry.value, entries[i].value);
        }
    }

    #[tokio::test]
    async fn test_table_reader_range_entries() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
            create_test_entry("key4", "value4", 4),
            create_test_entry("key5", "value5", 5),
        ];

        let (temp_file, _file_size) = create_test_sstable(entries.clone()).await;
        let table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        // Create range iterator for keys 2-4
        let start_key = entries[1].key.clone(); // key2
        let end_key = entries[3].key.clone(); // key4

        let range_entries = table_reader
            .range_iter(&start_key, &end_key)
            .await
            .expect("Failed to get range entries");
        assert_eq!(range_entries.len(), 3); // key2, key3, key4

        let expected_in_range = &entries[1..4]; // key2, key3, key4
        for (mut actual, expected) in range_entries.into_iter().zip(expected_in_range.iter()) {
            let entry = actual.entry().await.expect("Failed to get entry");
            assert_eq!(entry.key, expected.key);
            assert_eq!(entry.value, expected.value);
        }
    }

    #[tokio::test]
    async fn test_contains_key_range() {
        let entries = vec![
            create_test_entry("key2", "value2", 2),
            create_test_entry("key5", "value5", 5),
            create_test_entry("key8", "value8", 8),
        ];

        let (temp_file, _) = create_test_sstable(entries.clone()).await;
        let table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

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
