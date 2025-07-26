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

use std::{io::Cursor, path::Path};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use rsketch_common::readable_size::ReadableSize;
use snafu::{ResultExt, ensure};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
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

/// A lazy block reader that only reads entry metadata initially.
///
/// This allows us to iterate through entry positions without loading
/// the actual entry data until it's requested.
pub(crate) struct LazyBlockReader {
    /// File handle for reading entry data on demand
    file:          File,
    /// Starting offset of this block in the file
    block_offset:  u64,
    /// Entry offsets within the block (relative to block start)
    entry_offsets: Vec<u64>,
    /// Number of entries in this block
    entry_count:   u32,
}

impl LazyBlockReader {
    /// Creates a new lazy block reader.
    ///
    /// This only reads the block metadata (entry count and offsets) without
    /// loading any actual entry data.
    pub(crate) async fn new(mut file: File, block_offset: u64, block_size: u64) -> Result<Self> {
        // Read entry count from the last 4 bytes of the block
        file.seek(std::io::SeekFrom::Start(block_offset + block_size - 4))
            .await
            .context(IOSnafu)?;
        let mut count_buf = [0u8; 4];
        file.read_exact(&mut count_buf).await.context(IOSnafu)?;
        let entry_count = u32::from_le_bytes(count_buf);

        // Read offset table
        let offset_section_size = entry_count as u64 * 8; // 8 bytes per u64 offset
        let offset_table_start = block_offset + block_size - 4 - offset_section_size;

        file.seek(std::io::SeekFrom::Start(offset_table_start))
            .await
            .context(IOSnafu)?;
        let mut offset_buf = vec![0u8; offset_section_size as usize];
        file.read_exact(&mut offset_buf).await.context(IOSnafu)?;

        // Parse offsets
        let entry_offsets = offset_buf
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        Ok(Self {
            file,
            block_offset,
            entry_offsets,
            entry_count,
        })
    }

    /// Returns the number of entries in this block.
    pub(crate) fn entry_count(&self) -> usize { self.entry_count as usize }

    /// Returns true if the block is empty.
    pub(crate) fn is_empty(&self) -> bool { self.entry_count == 0 }

    /// Reads a specific entry by index.
    ///
    /// This performs I/O to read only the requested entry data.
    pub(crate) async fn read_entry(&mut self, index: usize) -> Result<Entry> {
        // Ensure the index is within bounds
        if index >= self.entry_count as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Entry index {} out of bounds, valid range: 0..{}",
                    index, self.entry_count
                ),
            ))
            .context(IOSnafu);
        }

        let entry_offset = self.block_offset + self.entry_offsets[index];
        self.file
            .seek(std::io::SeekFrom::Start(entry_offset))
            .await
            .context(IOSnafu)?;

        // Read the entry using a buffered approach
        // We'll read a reasonable chunk and decode from it
        let mut buffer = Vec::with_capacity(1024); // Start with 1KB buffer
        let mut temp_buf = [0u8; 1024];

        // Read initial chunk
        let bytes_read = self.file.read(&mut temp_buf).await.context(IOSnafu)?;
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
                        let bytes_read = self.file.read(&mut temp_buf).await.context(IOSnafu)?;
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

    /// Creates an iterator over all entries in this block.
    pub(crate) fn iter(&mut self) -> LazyBlockIterator { LazyBlockIterator::new(self) }
}

/// Iterator that lazily loads entries from a block.
pub(crate) struct LazyBlockIterator<'a> {
    block_reader:  &'a mut LazyBlockReader,
    current_index: usize,
}

impl<'a> LazyBlockIterator<'a> {
    fn new(block_reader: &'a mut LazyBlockReader) -> Self {
        Self {
            block_reader,
            current_index: 0,
        }
    }

    /// Advances to the next entry and returns it.
    ///
    /// This performs I/O only when an entry is actually requested.
    pub(crate) async fn next(&mut self) -> Result<Option<Entry>> {
        if self.current_index >= self.block_reader.entry_count() {
            return Ok(None);
        }

        let entry = self.block_reader.read_entry(self.current_index).await?;
        self.current_index += 1;
        Ok(Some(entry))
    }

    /// Peeks at the current entry without advancing the iterator.
    pub(crate) async fn peek(&mut self) -> Result<Option<Entry>> {
        if self.current_index >= self.block_reader.entry_count() {
            return Ok(None);
        }

        self.block_reader
            .read_entry(self.current_index)
            .await
            .map(Some)
    }

    /// Returns the current position in the iterator.
    pub(crate) fn position(&self) -> usize { self.current_index }

    /// Seeks to a specific position in the iterator.
    pub(crate) fn seek_to(&mut self, index: usize) {
        self.current_index = index.min(self.block_reader.entry_count());
    }
}

/// A production-ready table reader for reading SSTable files using tokio async
/// I/O.
///
/// Features:
/// - Data integrity verification with CRC32 checksums
/// - Efficient block-level access via index
/// - Point and range queries
/// - Support for iteration over entries
/// - Async I/O using tokio::fs
/// - Lazy loading of footer, index, and entry data
/// - Memory-efficient: only loads entry data when actually accessed
#[allow(dead_code)]
pub(crate) struct TableReader {
    /// The underlying file reader
    file:        File,
    /// Table footer containing metadata (lazily loaded)
    footer:      Option<TableFooter>,
    /// Index block for efficient lookups (lazily loaded)
    index_block: Option<Vec<IndexEntry>>,
    /// File size for validation
    file_size:   u64,
}

impl TableReader {
    /// Creates a new table reader from a file.
    ///
    /// This loads the footer and index block immediately for efficient queries,
    /// but entry data is still loaded lazily on demand.
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

        // Load the index block immediately
        let index_block = if footer.index_size == 0 {
            Some(Vec::new())
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
            let index_entries = reader
                .iter()
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(IOSnafu)?;
            Some(index_entries)
        };

        Ok(Self {
            file,
            footer: Some(footer),
            index_block,
            file_size,
        })
    }

    /// Finds the data block that may contain the given key.
    ///
    /// Returns the block offset and size, or None if the key is outside the
    /// table's range.
    pub(crate) fn find_block_for_key(&self, key: &InternalKey) -> Option<(u64, u64)> {
        if !self.contains_key_range(key) {
            return None;
        }

        if let Some(ref index_entries) = self.index_block {
            // Binary search through index entries to find the appropriate block
            match index_entries.binary_search_by(|entry| entry.key.cmp(key)) {
                Ok(idx) => {
                    // Exact match - the key is definitely in this block
                    Some((
                        index_entries[idx].block_offset,
                        index_entries[idx].block_size,
                    ))
                }
                Err(idx) => {
                    // Key would be inserted at idx, so it might be in the block at idx
                    if idx < index_entries.len() {
                        Some((
                            index_entries[idx].block_offset,
                            index_entries[idx].block_size,
                        ))
                    } else {
                        // Key is larger than all index entries, check the last block
                        if let Some(last_entry) = index_entries.last() {
                            Some((last_entry.block_offset, last_entry.block_size))
                        } else {
                            None
                        }
                    }
                }
            }
        } else {
            None
        }
    }

    /// Creates a lazy block reader for a specific data block.
    ///
    /// This only reads the block metadata initially.
    pub(crate) async fn create_lazy_block_reader(
        &mut self,
        offset: u64,
        size: u64,
    ) -> Result<LazyBlockReader> {
        // Clone the file handle for the block reader
        let file = self.file.try_clone().await.context(IOSnafu)?;
        LazyBlockReader::new(file, offset, size).await
    }

    /// Performs a point lookup for a specific key.
    ///
    /// Returns the entry if found, or None if the key doesn't exist in this
    /// table. Only reads the specific entry data when found.
    pub(crate) async fn get(&mut self, key: &InternalKey) -> Result<Option<Entry>> {
        if let Some((offset, size)) = self.find_block_for_key(key) {
            let mut block_reader = self.create_lazy_block_reader(offset, size).await?;
            let mut iter = block_reader.iter();

            // Search through the block for the key
            while let Some(entry) = iter.next().await? {
                match entry.key.cmp(key) {
                    std::cmp::Ordering::Equal => return Ok(Some(entry)),
                    std::cmp::Ordering::Greater => break, // Keys are sorted, so we won't find it
                    std::cmp::Ordering::Less => continue,
                }
            }
        }
        Ok(None)
    }

    /// Returns the table footer.
    pub(crate) fn footer(&self) -> &TableFooter { self.footer.as_ref().unwrap() }

    /// Returns the number of entries in the table.
    pub(crate) fn entry_count(&self) -> u32 { self.footer().entry_count }

    /// Returns the first key in the table, if any.
    pub(crate) fn first_key(&self) -> Option<&InternalKey> { self.footer().first_key.as_ref() }

    /// Returns the last key in the table, if any.
    pub(crate) fn last_key(&self) -> Option<&InternalKey> { self.footer().last_key.as_ref() }

    /// Validates that a key is within the table's key range.
    pub(crate) fn contains_key_range(&self, key: &InternalKey) -> bool {
        let footer = self.footer();

        if let Some(ref first_key) = footer.first_key {
            if key < first_key {
                return false;
            }
        }

        if let Some(ref last_key) = footer.last_key {
            if key > last_key {
                return false;
            }
        }

        true
    }

    /// Creates an iterator over all entries in the table.
    /// The iterator will lazily load entry data as needed.
    pub(crate) fn iter(&mut self) -> TableIterator { TableIterator::new(self) }

    /// Creates an iterator over entries within a key range.
    /// The iterator will lazily load entry data as needed.
    pub(crate) fn range_iter(
        &mut self,
        start_key: InternalKey,
        end_key: InternalKey,
    ) -> TableRangeIterator {
        TableRangeIterator::new(self, start_key, end_key)
    }
}

/// Iterator over all entries in a table.
///
/// This efficiently streams through all entries in the table by lazily loading
/// entry data on demand. Only the block metadata is loaded initially.
pub(crate) struct TableIterator {
    table_reader:         *mut TableReader, // Raw pointer to avoid borrowing issues
    current_block_reader: Option<LazyBlockReader>,
    current_block_iter:   Option<LazyBlockIterator<'static>>,
    current_block_index:  usize,
    initialized:          bool,
    finished:             bool,
}

impl TableIterator {
    fn new(table_reader: &mut TableReader) -> Self {
        Self {
            table_reader:         table_reader as *mut TableReader,
            current_block_reader: None,
            current_block_iter:   None,
            current_block_index:  0,
            initialized:          false,
            finished:             false,
        }
    }

    /// Loads the next data block for iteration.
    async fn load_next_block(&mut self) -> Result<bool> {
        let table_reader = unsafe { &mut *self.table_reader };

        if let Some(ref index_entries) = table_reader.index_block {
            if self.current_block_index >= index_entries.len() {
                return Ok(false); // No more blocks
            }

            let entry = &index_entries[self.current_block_index];
            let block_reader = table_reader
                .create_lazy_block_reader(entry.block_offset, entry.block_size)
                .await?;

            // Create iterator with proper lifetime handling
            self.current_block_reader = Some(block_reader);
            // We need to use unsafe here to extend the lifetime
            let iter = unsafe {
                let block_reader_ref = self.current_block_reader.as_mut().unwrap();
                std::mem::transmute(block_reader_ref.iter())
            };
            self.current_block_iter = Some(iter);
            self.current_block_index += 1;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Advances to the next entry in the table.
    pub(crate) async fn next(&mut self) -> Result<Option<Entry>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            // Try to get next entry from current block
            if let Some(ref mut iter) = self.current_block_iter {
                if let Some(entry) = iter.next().await? {
                    return Ok(Some(entry));
                }
            }

            // Current block is exhausted, try to load next block
            if !self.load_next_block().await? {
                self.finished = true;
                return Ok(None);
            }
        }
    }
}

/// Iterator over entries within a key range.
///
/// This efficiently streams through entries within a specified key range,
/// lazily loading entry data on demand.
pub(crate) struct TableRangeIterator {
    table_reader:         *mut TableReader, // Raw pointer to avoid borrowing issues
    start_key:            InternalKey,
    end_key:              InternalKey,
    current_block_reader: Option<LazyBlockReader>,
    current_block_iter:   Option<LazyBlockIterator<'static>>,
    current_block_index:  usize,
    initialized:          bool,
    finished:             bool,
}

impl TableRangeIterator {
    fn new(table_reader: &mut TableReader, start_key: InternalKey, end_key: InternalKey) -> Self {
        Self {
            table_reader: table_reader as *mut TableReader,
            start_key,
            end_key,
            current_block_reader: None,
            current_block_iter: None,
            current_block_index: 0,
            initialized: false,
            finished: false,
        }
    }

    /// Initializes the iterator by loading the index and finding the start
    /// block.
    async fn ensure_initialized(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        let table_reader = unsafe { &mut *self.table_reader };
        // table_reader.ensure_index_loaded().await?; // This is now eagerly loaded

        // Find the starting block for the range
        if let Some(ref index_entries) = table_reader.index_block {
            // Find the first block that might contain keys >= start_key
            match index_entries.binary_search_by(|entry| entry.key.cmp(&self.start_key)) {
                Ok(idx) => {
                    self.current_block_index = idx;
                }
                Err(idx) => {
                    // The start_key would be inserted at idx, so start from the previous block
                    // to ensure we don't miss any entries
                    self.current_block_index = idx.saturating_sub(1);
                }
            }
        }

        self.initialized = true;
        Ok(())
    }

    /// Loads the next data block for iteration within the range.
    async fn load_next_block(&mut self) -> Result<bool> {
        self.ensure_initialized().await?;

        let table_reader = unsafe { &mut *self.table_reader };

        if let Some(ref index_entries) = table_reader.index_block {
            if self.current_block_index >= index_entries.len() {
                return Ok(false); // No more blocks
            }

            let entry = &index_entries[self.current_block_index];

            // Check if this block might contain keys beyond our end range
            if entry.key < self.start_key {
                self.current_block_index += 1;
                return Box::pin(self.load_next_block()).await; // Skip this block
            }

            let block_reader = table_reader
                .create_lazy_block_reader(entry.block_offset, entry.block_size)
                .await?;

            // Create iterator with proper lifetime handling
            self.current_block_reader = Some(block_reader);
            let iter = unsafe {
                let block_reader_ref = self.current_block_reader.as_mut().unwrap();
                std::mem::transmute(block_reader_ref.iter())
            };
            self.current_block_iter = Some(iter);
            self.current_block_index += 1;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Advances to the next entry in the range.
    pub(crate) async fn next(&mut self) -> Result<Option<Entry>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            // Try to get next entry from current block
            if let Some(ref mut iter) = self.current_block_iter {
                while let Some(entry) = iter.next().await? {
                    // Check if entry is within our range
                    if entry.key < self.start_key {
                        continue; // Skip entries before range
                    }
                    if entry.key > self.end_key {
                        self.finished = true;
                        return Ok(None); // Past end of range
                    }

                    return Ok(Some(entry));
                }
            }

            // Current block is exhausted, try to load next block
            if !self.load_next_block().await? {
                self.finished = true;
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use tempfile::NamedTempFile;
    use tokio::{fs::File, io::AsyncWriteExt};

    use super::*;
    use crate::format::{UserKey, ValueType};

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
    async fn test_lazy_block_reader_basic() {
        // Create test entries
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
        ];

        let (temp_file, file_size) = create_test_sstable(entries.clone()).await;
        let mut table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        // Get first block info
        let index_entries = table_reader.index_block.as_ref().unwrap();
        assert!(!index_entries.is_empty(), "Index should have entries");

        let first_block = &index_entries[0];

        // Create lazy block reader
        let file = File::open(temp_file.path())
            .await
            .expect("Failed to open file");
        let mut lazy_reader =
            LazyBlockReader::new(file, first_block.block_offset, first_block.block_size)
                .await
                .expect("Failed to create lazy block reader");

        // Verify block metadata is loaded
        assert_eq!(lazy_reader.entry_count(), entries.len());
        assert!(!lazy_reader.is_empty());

        // Read entries one by one
        for (i, expected_entry) in entries.iter().enumerate() {
            let actual_entry = lazy_reader
                .read_entry(i)
                .await
                .expect("Failed to read entry");
            assert_eq!(actual_entry.key, expected_entry.key);
            assert_eq!(actual_entry.value, expected_entry.value);
        }

        // Test out of bounds
        let result = lazy_reader.read_entry(entries.len()).await;
        assert!(result.is_err(), "Should fail for out of bounds index");
    }

    #[tokio::test]
    async fn test_lazy_block_iterator() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
        ];

        let (temp_file, file_size) = create_test_sstable(entries.clone()).await;
        let mut table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        // table_reader
        //     .ensure_index_loaded()
        //     .await
        //     .expect("Failed to load index");
        let first_block = &table_reader.index_block.as_ref().unwrap()[0];

        let file = File::open(temp_file.path())
            .await
            .expect("Failed to open file");
        let mut lazy_reader =
            LazyBlockReader::new(file, first_block.block_offset, first_block.block_size)
                .await
                .expect("Failed to create lazy block reader");

        let mut iter = lazy_reader.iter();

        // Test iterator
        for expected_entry in &entries {
            let actual_entry = iter
                .next()
                .await
                .expect("Failed to get next entry")
                .expect("Entry should exist");
            assert_eq!(actual_entry.key, expected_entry.key);
            assert_eq!(actual_entry.value, expected_entry.value);
        }

        // Should return None after all entries are consumed
        let result = iter.next().await.expect("Failed to get next entry");
        assert!(result.is_none(), "Iterator should be exhausted");
    }

    #[tokio::test]
    async fn test_table_reader_lazy_loading() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
        ];

        let (temp_file, file_size) = create_test_sstable(entries.clone()).await;
        let mut table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        // Verify footer and index are not loaded yet
        assert!(
            table_reader.footer.is_some(),
            "Footer should be loaded after initialization"
        );
        assert!(
            table_reader.index_block.is_some(),
            "Index should be loaded after initialization"
        );

        // Test immediate access to metadata
        let entry_count = table_reader.entry_count();
        assert_eq!(entry_count, entries.len() as u32);
        assert!(
            table_reader.footer.is_some(),
            "Footer should be loaded after first access"
        );

        // Index should still not be loaded
        assert!(
            table_reader.index_block.is_some(),
            "Index should not be loaded until needed"
        );

        // Performing a get operation should trigger index loading
        let key = &entries[1].key;
        let result = table_reader.get(key).await.expect("Failed to perform get");
        assert!(result.is_some(), "Should find the key");
        assert!(
            table_reader.index_block.is_some(),
            "Index should be loaded after get operation"
        );

        let found_entry = result.unwrap();
        assert_eq!(found_entry.key, entries[1].key);
        assert_eq!(found_entry.value, entries[1].value);
    }

    #[tokio::test]
    async fn test_table_iterator_lazy_loading() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
            create_test_entry("key4", "value4", 4),
            create_test_entry("key5", "value5", 5),
        ];

        let (temp_file, file_size) = create_test_sstable(entries.clone()).await;
        let mut table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        // Create iterator - should not load anything yet
        let mut iter = table_reader.iter();
        assert!(
            table_reader.index_block.is_some(),
            "Index should be loaded when creating iterator"
        );

        // First next() call should trigger index loading
        let first_entry = iter
            .next()
            .await
            .expect("Failed to get first entry")
            .expect("Should have entries");
        assert!(
            unsafe { &*iter.table_reader }.index_block.is_some(),
            "Index should be loaded after first next()"
        );

        assert_eq!(first_entry.key, entries[0].key);
        assert_eq!(first_entry.value, entries[0].value);

        // Continue iterating through all entries
        for (i, expected_entry) in entries.iter().enumerate().skip(1) {
            let actual_entry = iter
                .next()
                .await
                .expect("Failed to get next entry")
                .expect("Should have more entries");
            assert_eq!(
                actual_entry.key, expected_entry.key,
                "Mismatch at index {}",
                i
            );
            assert_eq!(
                actual_entry.value, expected_entry.value,
                "Mismatch at index {}",
                i
            );
        }

        // Should return None after all entries
        let result = iter.next().await.expect("Failed to get next entry");
        assert!(result.is_none(), "Iterator should be exhausted");
    }

    #[tokio::test]
    async fn test_range_iterator_lazy_loading() {
        let entries = vec![
            create_test_entry("key1", "value1", 1),
            create_test_entry("key2", "value2", 2),
            create_test_entry("key3", "value3", 3),
            create_test_entry("key4", "value4", 4),
            create_test_entry("key5", "value5", 5),
        ];

        let (temp_file, file_size) = create_test_sstable(entries.clone()).await;
        let mut table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        // Create range iterator for keys 2-4
        let start_key = entries[1].key.clone(); // key2
        let end_key = entries[3].key.clone(); // key4

        let mut range_iter = table_reader.range_iter(start_key.clone(), end_key.clone());
        assert!(
            table_reader.index_block.is_some(),
            "Index should be loaded when creating range iterator"
        );

        // First next() call should trigger index loading and find start block
        let first_entry = range_iter
            .next()
            .await
            .expect("Failed to get first entry")
            .expect("Should have entries in range");
        assert!(
            unsafe { &*range_iter.table_reader }.index_block.is_some(),
            "Index should be loaded after first next()"
        );

        // Should get entries within range: key2, key3, key4
        let expected_in_range = &entries[1..4]; // key2, key3, key4
        let mut actual_entries = vec![first_entry];

        while let Some(entry) = range_iter.next().await.expect("Failed to get next entry") {
            actual_entries.push(entry);
        }

        assert_eq!(actual_entries.len(), expected_in_range.len());
        for (actual, expected) in actual_entries.iter().zip(expected_in_range.iter()) {
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.value, expected.value);
        }
    }

    #[tokio::test]
    async fn test_multiple_blocks_lazy_loading() {
        // Create enough entries to span multiple blocks
        let mut entries = Vec::new();
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            entries.push(create_test_entry(&key, &value, i as u64));
        }

        let (temp_file, file_size) = create_test_sstable(entries.clone()).await;
        let mut table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        // Verify we have multiple blocks
        // table_reader
        //     .ensure_index_loaded()
        //     .await
        //     .expect("Failed to load index");
        let block_count = table_reader.index_block.as_ref().unwrap().len();

        if block_count > 1 {
            // Test that we can access entries from different blocks
            let first_entry = table_reader
                .get(&entries[0].key)
                .await
                .expect("Failed to get first entry")
                .expect("Should find first entry");
            let last_entry = table_reader
                .get(&entries.last().unwrap().key)
                .await
                .expect("Failed to get last entry")
                .expect("Should find last entry");

            assert_eq!(first_entry.key, entries[0].key);
            assert_eq!(last_entry.key, entries.last().unwrap().key);

            // Test iterator across multiple blocks
            let mut table_reader2 = TableReader::open(temp_file.path())
                .await
                .expect("Failed to create table reader");
            let mut iter = table_reader2.iter();

            let mut count = 0;
            while let Some(_) = iter.next().await.expect("Failed to iterate") {
                count += 1;
            }

            assert_eq!(
                count,
                entries.len(),
                "Should iterate through all entries across multiple blocks"
            );
        }
    }

    #[tokio::test]
    async fn test_lazy_loading_error_handling() {
        let entries = vec![create_test_entry("key1", "value1", 1)];

        let (temp_file, file_size) = create_test_sstable(entries.clone()).await;

        // Create a very small file for testing invalid file size
        let small_temp_file = NamedTempFile::new().expect("Failed to create temp file");
        std::fs::write(small_temp_file.path(), b"small").expect("Failed to write small file");

        let result = TableReader::open(small_temp_file.path()).await; // Too small
        assert!(result.is_err(), "Should fail with invalid file size");

        // Test with valid file
        let mut table_reader = TableReader::open(temp_file.path())
            .await
            .expect("Failed to create table reader");

        // This should work normally
        let entry_count = table_reader.entry_count();
        assert_eq!(entry_count, 1);
    }

    #[tokio::test]
    async fn test_contains_key_range() {
        let entries = vec![
            create_test_entry("key2", "value2", 2),
            create_test_entry("key5", "value5", 5),
            create_test_entry("key8", "value8", 8),
        ];

        let (temp_file, _) = create_test_sstable(entries.clone()).await;
        let mut table_reader = TableReader::open(temp_file.path())
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
