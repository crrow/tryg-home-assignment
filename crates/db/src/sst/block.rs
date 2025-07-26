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

use std::{
    collections::HashMap,
    fs::File,
    hash::Hash,
    io::{IoSlice, Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
};

use byteorder::ReadBytesExt;
use snafu::ResultExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use value_log::Slice;

use crate::{
    format::{Codec, Entry, IndexEntry, InternalKey},
    sst::err::{IOSnafu, Result},
};

/// Trait for types that can be stored in a block.
///
/// Block entries must be:
/// - Serializable via the Codec trait
/// - Have a key that can be extracted for ordering
/// - Have a computable size for block size estimation
pub trait BlockEntry: Codec + Clone + PartialEq {
    /// The type of key used for ordering entries in the block
    type Key: Ord + Clone + std::fmt::Debug + std::hash::Hash + Eq;

    /// Extract the key from this entry for ordering purposes
    fn key(&self) -> &Self::Key;

    /// Return the serialized size of this entry in bytes
    fn size(&self) -> u64;
}

/// Implementation of BlockEntry for the existing Entry type (data blocks)
impl BlockEntry for Entry {
    type Key = InternalKey;

    fn key(&self) -> &Self::Key { &self.key }

    fn size(&self) -> u64 { self.size() }
}

/// Implementation of BlockEntry for IndexEntry type (index blocks)
impl BlockEntry for IndexEntry {
    type Key = InternalKey;

    fn key(&self) -> &Self::Key { &self.key }

    /// Returns the serialized size of this IndexEntry in bytes.
    /// This includes:
    /// - The serialized size of the key
    /// - 8 bytes for block_offset (u64)
    /// - 8 bytes for block_size (u64)
    fn size(&self) -> u64 { self.key.size() + 8 + 8 }
}

/// A lazy-loading entry that holds only the key until data is explicitly
/// requested.
///
/// This struct allows for memory-efficient handling of entries by storing only
/// the key and the information needed to read the full entry data when
/// required. The actual entry data is loaded on-demand through the reader.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IOEntry<R: std::io::Read + std::io::Seek> {
    /// The key of this entry, available immediately without I/O
    key:          InternalKey,
    /// Reader positioned at the start of this entry's data
    reader:       R,
    /// Byte offset where this entry's data begins in the reader
    offset:       u64,
    /// Cached entry data, loaded on first access
    cached_entry: Option<Entry>,
}

impl<R: std::io::Read + std::io::Seek> IOEntry<R> {
    /// Creates a new IOEntry by reading the key from the reader at the given
    /// offset.
    ///
    /// # Parameters
    /// * `reader` - Reader that can seek to and read the entry's data
    /// * `offset` - Byte offset where this entry's data begins
    pub(crate) fn new(mut reader: R, offset: u64) -> std::io::Result<Self> {
        // Seek to the entry's position
        reader.seek(SeekFrom::Start(offset))?;
        // Parse the key from the entry data
        let key = InternalKey::decode_from(&mut reader)?;

        // Reset reader position for later use
        reader.seek(SeekFrom::Start(offset))?;

        Ok(Self {
            key,
            reader,
            offset,
            cached_entry: None,
        })
    }

    /// Loads and returns the full entry data, performing I/O if not already
    /// cached.
    ///
    /// This method will seek to the entry's position and decode the data on
    /// first call, then cache the result for subsequent calls.
    pub(crate) fn entry(&mut self) -> std::io::Result<&Entry> {
        if self.cached_entry.is_none() {
            // Seek to the entry's position
            self.reader.seek(SeekFrom::Start(self.offset))?;

            // Decode the entry from the reader
            let entry = Entry::decode_from(&mut self.reader)?;
            self.cached_entry = Some(entry);
        }

        // Return the cached entry (safe to unwrap since we just ensured it exists)
        Ok(self.cached_entry.as_ref().unwrap())
    }
}

/// An async version of IOEntry that works with any async reader.
///
/// This struct stores the exact offset of the entry data, allowing direct
/// seeking to the entry without rebuilding block readers.
#[derive(PartialEq, Eq)]
pub(crate) struct AsyncIOEntry<R: tokio::io::AsyncRead + tokio::io::AsyncSeek + Unpin> {
    /// The key of this entry
    key:          InternalKey,
    /// Shared async reader
    reader:       R,
    /// Exact byte offset where this entry's data starts in the reader
    entry_offset: u64,
    /// Cached entry data, loaded on first access
    cached_entry: Option<Entry>,
}

impl<R: tokio::io::AsyncRead + tokio::io::AsyncSeek + Unpin> Hash for AsyncIOEntry<R> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.key.hash(state); }
}

impl<R> AsyncIOEntry<R>
where
    R: tokio::io::AsyncRead + tokio::io::AsyncSeek + Unpin,
{
    /// Creates a new AsyncIOEntry with the exact entry offset.
    ///
    /// # Parameters
    /// * `reader` - Async reader that can seek to and read the entry's data
    /// * `entry_offset` - Exact byte offset where this entry's data starts
    pub(crate) async fn new(mut reader: R, entry_offset: u64) -> Result<Self> {
        reader
            .seek(SeekFrom::Start(entry_offset))
            .await
            .context(IOSnafu)?;
        let key = InternalKey::async_decode_from(&mut reader)
            .await
            .context(IOSnafu)?;
        Ok(Self {
            key,
            reader,
            entry_offset,
            cached_entry: None,
        })
    }

    /// Loads and returns the full entry data, performing async I/O if not
    /// already cached.
    ///
    /// This method seeks directly to the entry's offset and decodes it,
    /// without needing to rebuild block readers.
    pub(crate) async fn entry(&mut self) -> std::io::Result<&Entry> {
        if self.cached_entry.is_none() {
            // Seek directly to the entry's offset
            self.reader
                .seek(std::io::SeekFrom::Start(self.entry_offset))
                .await?;

            // Decode the entry directly from the reader
            let entry = Entry::async_decode_from(&mut self.reader).await?;
            self.cached_entry = Some(entry);
        }

        Ok(self.cached_entry.as_ref().unwrap())
    }
}

/// A generic block builder that can build blocks for different entry types.
///
/// ## Block Layout
///
/// ```text
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
/// ```
///
/// The block builder accumulates entries and their offsets, then can be
/// finished to produce the final block bytes.
#[derive(Debug, Clone)]
pub(crate) struct BlockBuilder<T: BlockEntry> {
    /// The entries stored in this block
    items:          Vec<T>,
    /// Byte offsets for each item in the data section
    offsets:        Vec<u64>,
    /// Current offset in the data section
    current_offset: u64,
    /// The last key in the block
    last_key:       Option<T::Key>,
    /// Whether this builder has been finished and can no longer accept new
    /// items
    finished:       bool,
}

impl<T: BlockEntry> BlockBuilder<T> {
    /// Creates a new empty block builder.
    pub(crate) fn new() -> Self {
        Self {
            items:          Vec::new(),
            offsets:        Vec::new(),
            current_offset: 0,
            last_key:       None,
            finished:       false,
        }
    }

    /// Adds a new entry to the block.
    ///
    /// Returns an error if the block builder has already been finished.
    ///
    /// # Panics
    ///
    /// Panics if entries are not added in sorted order by key.
    pub(crate) fn add(&mut self, entry: T) {
        assert!(!self.finished, "BlockBuilder should not be finished");

        // Ensure keys are added in sorted order
        if let Some(ref last_key) = self.last_key {
            assert!(
                entry.key() > last_key,
                "Keys must be added in sorted order: new key {:?} is not greater than last key \
                 {:?}",
                entry.key(),
                last_key
            );
        }

        self.offsets.push(self.current_offset);
        self.current_offset += entry.size();
        self.last_key = Some(entry.key().clone());
        self.items.push(entry);
    }

    /// Returns the current number of items in the block.
    pub(crate) fn len(&self) -> usize { self.items.len() }

    /// Returns true if the block is empty.
    pub(crate) fn is_empty(&self) -> bool { self.items.is_empty() }

    /// Estimates the total size of the block in bytes when finished.
    ///
    /// This includes:
    /// - Size of all data entries
    /// - Size of offset table (8 bytes per entry)
    /// - Size of entry count (4 bytes)
    pub(crate) fn estimate_size(&self) -> u64 {
        const ENTRY_COUNT_SIZE: u64 = std::mem::size_of::<u32>() as u64;
        const OFFSET_SIZE: u64 = std::mem::size_of::<u64>() as u64;

        let num_items = self.items.len() as u64;
        let offset_table_size = num_items * OFFSET_SIZE;
        let data_section_size = self.current_offset;

        data_section_size + offset_table_size + ENTRY_COUNT_SIZE
    }

    /// Marks the block builder as finished, preventing further modifications.
    pub(crate) fn finish(&mut self) { self.finished = true; }

    /// Returns whether the block builder has been finished.
    pub(crate) fn is_finished(&self) -> bool { self.finished }

    /// Clears all items from the block builder and resets it to an unfinished
    /// state.
    pub(crate) fn clear(&mut self) {
        self.items.clear();
        self.offsets.clear();
        self.current_offset = 0;
        self.last_key = None;
        self.finished = false;
    }

    /// Returns an iterator over the items in the block.
    pub(crate) fn items(&self) -> impl Iterator<Item = &T> { self.items.iter() }

    /// Returns a slice of the offsets for each item.
    pub(crate) fn offsets(&self) -> &[u64] { &self.offsets }

    /// Writes the block to the provided writer.
    ///
    /// The block format is:
    /// - Data section: serialized entries using their Codec implementation
    /// - Offset section: u64 offsets for each entry (little-endian)
    /// - Entry count: u32 number of entries (little-endian)
    ///
    /// Returns an error if the block is not finished or is empty.
    pub(crate) fn build_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        if !self.finished {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Block must be finished before building",
            ));
        }

        if self.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot build empty block",
            ));
        }

        // Write data section - serialize each entry using its Codec implementation
        for item in &self.items {
            item.encode_into(writer)?;
        }

        // Write offset section - each offset as little-endian u64
        for &offset in &self.offsets {
            writer.write_all(&offset.to_le_bytes())?;
        }

        // Write entry count as little-endian u32
        writer.write_all(&(self.items.len() as u32).to_le_bytes())?;

        Ok(())
    }

    /// Convenience method to build the block into a byte vector.
    ///
    /// This is a wrapper around `build_into` for cases where you need the bytes
    /// in memory. Use `build_into` directly when possible to avoid allocation.
    pub(crate) fn build(&self) -> std::io::Result<Vec<u8>> {
        let mut result = Vec::new();
        self.build_into(&mut result)?;
        Ok(result)
    }
}

/// Type alias for backward compatibility with existing data block usage
pub(crate) type DataBlockBuilder = BlockBuilder<Entry>;

/// Type alias for index block builder
pub(crate) type IndexBlockBuilder = BlockBuilder<IndexEntry>;

/// A reader for reading a block from a reader.
pub(crate) struct BlockReader<T: BlockEntry, R> {
    /// The reader for the block
    reader:  R,
    /// The offsets for each entry in the block
    offsets: Vec<u64>,
    /// The number of entries in the block, used for iteration
    count:   u32,
    _marker: std::marker::PhantomData<T>,
}

impl<T: BlockEntry, R> Clone for BlockReader<T, R>
where
    R: Clone,
{
    fn clone(&self) -> Self {
        Self {
            reader:  self.reader.clone(),
            offsets: self.offsets.clone(),
            count:   self.count,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: BlockEntry, R: std::io::Read + std::io::Seek> BlockReader<T, R> {
    pub(crate) fn new(mut reader: R) -> Result<Self> {
        // Try to read from the end to get the entry count
        let mut buf = [0; 4];
        reader.seek(SeekFrom::End(-4)).context(IOSnafu)?;
        reader.read_exact(&mut buf).context(IOSnafu)?;

        // how many entry in this block.
        let count = u32::from_le_bytes(buf);
        // the offset section size is the number of entries * the size of each offset.
        let offset_section_size = count as usize * std::mem::size_of::<u64>();
        let mut offset_buf = vec![0; offset_section_size];
        // Read offset table from the position before the entry count.
        reader
            .seek(SeekFrom::End(-4 - offset_section_size as i64))
            .context(IOSnafu)?;
        reader.read_exact(&mut offset_buf).context(IOSnafu)?;
        let offsets: Vec<u64> = offset_buf
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        Ok(Self {
            reader,
            offsets,
            count,
            _marker: std::marker::PhantomData,
        })
    }

    /// Returns an iterator over the entries in this block.
    pub(crate) fn iter(&mut self) -> BlockIterator<'_, T, R> {
        BlockIterator {
            reader:        &mut self.reader,
            offsets:       &self.offsets,
            current_index: 0,
            count:         self.count,
            _marker:       std::marker::PhantomData,
        }
    }

    /// Returns the number of entries in this block.
    pub(crate) fn len(&self) -> usize { self.count as usize }

    /// Returns true if the block is empty.
    pub(crate) fn is_empty(&self) -> bool { self.count == 0 }

    /// Performs a point lookup for a specific key within this block.
    ///
    /// Searches through entries sequentially until the key is found or we
    /// determine it doesn't exist. Since entries are sorted by key, we can
    /// stop early if we encounter a key greater than our target.
    ///
    /// # Parameters
    /// * `target_key` - The key to search for
    ///
    /// # Returns
    /// * `Ok(Some(entry))` - If the key is found
    /// * `Ok(None)` - If the key is not found in this block
    /// * `Err(...)` - If there's an I/O error while reading
    pub(crate) fn get(&mut self, target_key: &T::Key) -> std::io::Result<Option<T>> {
        // Search through all entries sequentially
        for index in 0..self.count as usize {
            let offset = self.offsets[index];
            self.reader.seek(SeekFrom::Start(offset))?;

            // Decode the entry to check its key
            let entry = T::decode_from(&mut self.reader)?;

            match entry.key().cmp(target_key) {
                std::cmp::Ordering::Equal => return Ok(Some(entry)),
                std::cmp::Ordering::Greater => {
                    // Since entries are sorted, we won't find the target key
                    // in the remaining entries
                    return Ok(None);
                }
                std::cmp::Ordering::Less => {
                    // Continue searching
                    continue;
                }
            }
        }

        // Key not found after checking all entries
        Ok(None)
    }

    /// Gets an entry at a specific index within this block.
    ///
    /// This is useful when you know the exact position of the entry you want.
    /// Returns None if the index is out of bounds.
    ///
    /// # Parameters
    /// * `index` - Zero-based index of the entry to retrieve
    ///
    /// # Returns
    /// * `Ok(Some(entry))` - If the index is valid
    /// * `Ok(None)` - If the index is out of bounds
    /// * `Err(...)` - If there's an I/O error while reading
    pub(crate) fn get_at_index(&mut self, index: usize) -> std::io::Result<Option<T>> {
        if index >= self.count as usize {
            return Ok(None);
        }

        // Seek to the entry at the specified index
        let offset = self.offsets[index];
        self.reader.seek(SeekFrom::Start(offset))?;

        // Decode and return the entry
        let entry = T::decode_from(&mut self.reader)?;
        Ok(Some(entry))
    }
}

/// Type alias for backward compatibility with existing data block usage
pub(crate) type DataBlockReader<R> = BlockReader<Entry, R>;

/// Type alias for index block reader
pub(crate) type IndexBlockReader<R> = BlockReader<IndexEntry, R>;

/// Iterator over the entries in a block.
pub(crate) struct BlockIterator<'a, T: BlockEntry, R: std::io::Read + std::io::Seek> {
    reader:        &'a mut R,
    offsets:       &'a [u64],
    current_index: usize,
    count:         u32,
    _marker:       std::marker::PhantomData<T>,
}

impl<'a, T: BlockEntry, R: std::io::Read + std::io::Seek> Iterator for BlockIterator<'a, T, R> {
    type Item = std::io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.count as usize {
            return None;
        }

        let offset = self.offsets[self.current_index];

        // Seek to the position of the current entry
        if let Err(e) = self.reader.seek(SeekFrom::Start(offset)) {
            return Some(Err(e));
        }

        // Decode the entry from the reader using its Codec implementation
        match T::decode_from(self.reader) {
            Ok(entry) => {
                self.current_index += 1;
                Some(Ok(entry))
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count as usize - self.current_index;
        (remaining, Some(remaining))
    }
}

impl<'a, T: BlockEntry, R: std::io::Read + std::io::Seek> ExactSizeIterator
    for BlockIterator<'a, T, R>
{
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use test_case::test_case;

    use super::*;
    use crate::format::{Entry, UserKey, ValueType};

    /// A mock writer that counts bytes written without storing them
    struct CountingWriter {
        bytes_written: usize,
    }

    impl CountingWriter {
        fn new() -> Self { Self { bytes_written: 0 } }

        fn bytes_written(&self) -> usize { self.bytes_written }
    }

    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.bytes_written += buf.len();
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    /// Helper function to create test InternalValue
    fn create_test_value(key: &str, timestamp: u64, seqno: u64, value: &str) -> Entry {
        Entry::make(key, timestamp, seqno, value)
    }

    /// Helper function to create tombstone InternalValue
    fn create_tombstone(key: &str, timestamp: u64, seqno: u64) -> Entry {
        Entry {
            key:   InternalKey::new(
                UserKey::builder().key(key).timestamp(timestamp).build(),
                seqno,
                ValueType::Tombstone,
            ),
            value: value_log::Slice::from(vec![]),
        }
    }

    #[test]
    fn test_block_builder_new() {
        let builder = BlockBuilder::<Entry>::new();
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);
        assert!(!builder.is_finished());
        assert_eq!(builder.estimate_size(), 4); // Just the entry count
    }

    #[test_case("key1", 100, 1, "value1"; "simple value")]
    #[test_case("", 0, 0, ""; "empty strings")]
    #[test_case("very_long_key_name_that_tests_boundaries", 999999, 12345, "very_long_value_content_to_test_size_calculations"; "long strings")]
    #[test_case("特殊字符", 100, 1, "测试"; "unicode characters")]
    fn test_block_builder_add_single_item(key: &str, timestamp: u64, seqno: u64, value: &str) {
        let mut builder = BlockBuilder::<Entry>::new();
        let test_value = create_test_value(key, timestamp, seqno, value);

        builder.add(test_value.clone());

        assert!(!builder.is_empty());
        assert_eq!(builder.len(), 1);
        assert_eq!(builder.offsets(), &[0]);

        let items: Vec<_> = builder.items().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], &test_value);
    }

    #[test]
    fn test_block_builder_add_multiple_items() {
        let mut builder = BlockBuilder::<Entry>::new();
        let value1 = create_test_value("key1", 100, 3, "value1");
        let value2 = create_test_value("key2", 100, 2, "value2");
        let value3 = create_test_value("key3", 100, 1, "value3");

        builder.add(value1.clone());
        builder.add(value2.clone());
        builder.add(value3.clone());

        assert_eq!(builder.len(), 3);
        assert_eq!(builder.offsets().len(), 3);

        // Verify offsets are cumulative
        let offsets = builder.offsets();
        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1], value1.size());
        assert_eq!(offsets[2], value1.size() + value2.size());
    }

    #[test_case("key2", "key1", 100, 100, 2, 1; "alphabetical descending")]
    #[test_case("z", "a", 100, 100, 2, 1; "single character descending")]
    #[test_case("key1", "key1", 100, 100, 1, 2; "same key, wrong seqno order")]
    #[test_case("key1", "key1", 100, 200, 1, 2; "same key, wrong timestamp order")]
    #[should_panic(expected = "Keys must be added in sorted order")]
    fn test_block_builder_enforces_sort_order(
        first_key: &str,
        second_key: &str,
        first_timestamp: u64,
        second_timestamp: u64,
        first_seqno: u64,
        second_seqno: u64,
    ) {
        let mut builder = BlockBuilder::<Entry>::new();
        let value1 = create_test_value(first_key, first_timestamp, first_seqno, "value1");
        let value2 = create_test_value(second_key, second_timestamp, second_seqno, "value2");

        builder.add(value1);
        builder.add(value2); // Should panic for all test cases
    }

    #[test_case("key1", "key2", 100, 100, 2, 1; "alphabetical ascending")]
    #[test_case("a", "z", 100, 100, 2, 1; "single character ascending")]
    #[test_case("key1", "key1", 100, 100, 2, 1; "same key, correct seqno order (desc)")]
    #[test_case("key1", "key1", 200, 100, 2, 1; "same key, correct timestamp order (desc by timestamp)")]
    fn test_block_builder_accepts_correct_order(
        first_key: &str,
        second_key: &str,
        first_timestamp: u64,
        second_timestamp: u64,
        first_seqno: u64,
        second_seqno: u64,
    ) {
        let mut builder = BlockBuilder::<Entry>::new();
        let value1 = create_test_value(first_key, first_timestamp, first_seqno, "value1");
        let value2 = create_test_value(second_key, second_timestamp, second_seqno, "value2");

        builder.add(value1);
        builder.add(value2); // Should succeed for all test cases

        assert_eq!(builder.len(), 2);
    }

    #[test]
    #[should_panic(expected = "BlockBuilder should not be finished")]
    fn test_block_builder_prevents_add_after_finish() {
        let mut builder = BlockBuilder::<Entry>::new();
        let value = create_test_value("key1", 100, 1, "value1");

        builder.add(value.clone());
        builder.finish();
        builder.add(value); // Should panic
    }

    #[test]
    fn test_block_builder_estimate_size() {
        let mut builder = BlockBuilder::<Entry>::new();
        let initial_size = builder.estimate_size();

        let value1 = create_test_value("key1", 100, 1, "value1");
        builder.add(value1.clone());

        let expected_size = initial_size + value1.size() + 8; // +8 for offset
        assert_eq!(builder.estimate_size(), expected_size);
    }

    #[test]
    fn test_block_builder_clear() {
        let mut builder = BlockBuilder::<Entry>::new();
        let value = create_test_value("key1", 100, 1, "value1");

        builder.add(value);
        builder.finish();
        assert!(!builder.is_empty());
        assert!(builder.is_finished());

        builder.clear();
        assert!(builder.is_empty());
        assert!(!builder.is_finished());
        assert_eq!(builder.len(), 0);
    }

    #[test_case(true, false; "empty finished block")]
    #[test_case(false, true; "unfinished block with data")]
    fn test_block_build_error_conditions(should_finish: bool, should_add_data: bool) {
        let mut builder = BlockBuilder::<Entry>::new();

        if should_add_data {
            let value = create_test_value("key1", 100, 1, "value1");
            builder.add(value);
        }

        if should_finish {
            builder.finish();
        }

        // Test both build methods for consistency
        let result = builder.build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        let mut buffer = Vec::new();
        let result_into = builder.build_into(&mut buffer);
        assert!(result_into.is_err());
        assert_eq!(
            result_into.unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn test_build_into_different_writers() {
        let mut builder = BlockBuilder::<Entry>::new();

        // Add test data
        let values = vec![
            create_test_value("apple", 100, 3, "fruit"),
            create_test_value("banana", 200, 2, "yellow"),
            create_test_value("cherry", 150, 1, "red"),
        ];

        for value in &values {
            builder.add(value.clone());
        }
        builder.finish();

        // Test writing to Vec<u8>
        let mut vec_buffer = Vec::new();
        builder
            .build_into(&mut vec_buffer)
            .expect("Failed to write to Vec");

        // Test writing to Cursor
        let mut cursor_buffer = Vec::new();
        {
            let mut cursor = Cursor::new(&mut cursor_buffer);
            builder
                .build_into(&mut cursor)
                .expect("Failed to write to Cursor");
        }

        // Both should produce identical results
        assert_eq!(vec_buffer, cursor_buffer);

        // Verify we can read back the data
        let cursor = Cursor::new(vec_buffer);
        let mut reader = DataBlockReader::new(cursor).expect("Failed to create reader");

        let read_values: std::result::Result<Vec<_>, std::io::Error> = reader.iter().collect();
        let read_values = read_values.expect("Failed to read values");

        assert_eq!(read_values.len(), values.len());
        for (original, read) in values.iter().zip(read_values.iter()) {
            assert_eq!(original, read);
        }
    }

    #[test]
    fn test_block_builder_and_reader_roundtrip() {
        let mut builder = BlockBuilder::<Entry>::new();

        // Add test data
        let values = vec![
            create_test_value("apple", 100, 5, "fruit"),
            create_test_value("banana", 200, 4, "yellow"),
            create_tombstone("cherry", 150, 3),
            create_test_value("date", 300, 2, "sweet"),
            create_test_value("elderberry", 250, 1, "purple"),
        ];

        for value in &values {
            builder.add(value.clone());
        }
        builder.finish();

        // Build the block
        let block_bytes = builder.build().expect("Failed to build block");

        // Create reader from the bytes
        let cursor = Cursor::new(block_bytes);
        let mut reader = DataBlockReader::new(cursor).expect("Failed to create reader");

        // Verify reader properties
        assert_eq!(reader.len(), values.len());
        assert!(!reader.is_empty());

        // Read back all values using iterator
        let read_values: std::result::Result<Vec<_>, std::io::Error> = reader.iter().collect();
        let read_values = read_values.expect("Failed to read values");

        // Verify all values match
        assert_eq!(read_values.len(), values.len());
        for (original, read) in values.iter().zip(read_values.iter()) {
            assert_eq!(original, read);
        }
    }

    #[test]
    fn test_build_into_memory_efficiency() {
        let mut builder = BlockBuilder::<Entry>::new();

        // Add test data
        for i in 0..100 {
            let value =
                create_test_value(&format!("key{i:04}"), 100, i as u64, &format!("value{i}"));
            builder.add(value);
        }
        builder.finish();

        // Test with counting writer (no memory allocation for data)
        let mut counting_writer = CountingWriter::new();
        builder
            .build_into(&mut counting_writer)
            .expect("Failed to write to CountingWriter");

        // Test with Vec (allocates memory)
        let vec_result = builder.build().expect("Failed to build to Vec");

        // Both should write the same number of bytes
        assert_eq!(counting_writer.bytes_written(), vec_result.len());

        // The estimated size should be close to actual size
        let estimated_size = builder.estimate_size();
        assert_eq!(estimated_size, vec_result.len() as u64);
    }

    #[test_case(0; "empty iterator")]
    #[test_case(1; "single item iterator")]
    #[test_case(5; "multiple item iterator")]
    #[test_case(100; "large iterator")]
    fn test_block_iterator_size_hint(num_items: usize) {
        let mut builder = BlockBuilder::<Entry>::new();

        // Add test data
        for i in 0..num_items {
            let value = create_test_value(&format!("key{i:06}"), 100, i as u64, "value");
            builder.add(value);
        }

        if num_items > 0 {
            builder.finish();
            let block_bytes = builder.build().expect("Failed to build block");
            let cursor = Cursor::new(block_bytes);
            let mut reader = DataBlockReader::new(cursor).expect("Failed to create reader");
            let mut iter = reader.iter();

            // Test size hint at start
            assert_eq!(iter.size_hint(), (num_items, Some(num_items)));
            assert_eq!(iter.len(), num_items);

            // Consume items one by one and check size hint
            for remaining in (0..num_items).rev() {
                if remaining > 0 {
                    iter.next();
                    assert_eq!(iter.size_hint(), (remaining, Some(remaining)));
                    assert_eq!(iter.len(), remaining);
                }
            }
        } else {
            // Test empty block case
            let mut block_bytes = Vec::new();
            block_bytes.extend_from_slice(&0u32.to_le_bytes());

            let cursor = Cursor::new(block_bytes);
            let mut reader = DataBlockReader::new(cursor).expect("Failed to create reader");
            let iter = reader.iter();

            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.len(), 0);
        }
    }

    #[test]
    fn test_empty_block_reader() {
        // Create a block with minimal valid structure (just entry count = 0)
        let mut block_bytes = Vec::new();
        block_bytes.extend_from_slice(&0u32.to_le_bytes()); // 0 entries

        let cursor = Cursor::new(block_bytes);
        let mut reader = DataBlockReader::new(cursor).expect("Failed to create reader");

        assert_eq!(reader.len(), 0);
        assert!(reader.is_empty());

        let items: Vec<_> = reader.iter().collect();
        assert!(items.is_empty());
    }

    #[test_case(vec![false, true, false, true]; "alternating values and tombstones")]
    #[test_case(vec![true, true, true]; "all tombstones")]
    #[test_case(vec![false, false, false]; "all values")]
    #[test_case(vec![true]; "single tombstone")]
    #[test_case(vec![false]; "single value")]
    fn test_block_with_mixed_entry_types(is_tombstone_pattern: Vec<bool>) {
        let mut builder = BlockBuilder::<Entry>::new();

        // Create entries based on the pattern
        for (i, &is_tombstone) in is_tombstone_pattern.iter().enumerate() {
            let key = format!("key{}", i + 1);
            let timestamp = 100 + (i as u64 * 50);
            let seqno = (is_tombstone_pattern.len() - i) as u64;

            if is_tombstone {
                builder.add(create_tombstone(&key, timestamp, seqno));
            } else {
                builder.add(create_test_value(
                    &key,
                    timestamp,
                    seqno,
                    &format!("value{}", i + 1),
                ));
            }
        }

        builder.finish();
        let block_bytes = builder.build().expect("Failed to build block");

        let cursor = Cursor::new(block_bytes);
        let mut reader = DataBlockReader::new(cursor).expect("Failed to create reader");

        let values: std::result::Result<Vec<_>, std::io::Error> = reader.iter().collect();
        let values = values.expect("Failed to read values");

        assert_eq!(values.len(), is_tombstone_pattern.len());

        // Verify the tombstone pattern matches
        for (i, &expected_tombstone) in is_tombstone_pattern.iter().enumerate() {
            assert_eq!(
                values[i].key.is_tombstone(),
                expected_tombstone,
                "Entry {i} tombstone status mismatch"
            );
        }
    }

    #[test_case(1; "single entry")]
    #[test_case(10; "small block")]
    #[test_case(100; "medium block")]
    #[test_case(1000; "large block")]
    fn test_block_with_varying_sizes(num_entries: usize) {
        let mut builder = BlockBuilder::<Entry>::new();

        // Add entries to test offset calculations
        for i in 0..num_entries {
            let key = format!("key{i:06}");
            let value = format!("value{i}");
            builder.add(create_test_value(&key, 100, i as u64, &value));
        }

        builder.finish();
        let block_bytes = builder.build().expect("Failed to build block");

        let cursor = Cursor::new(block_bytes);
        let mut reader = DataBlockReader::new(cursor).expect("Failed to create reader");

        assert_eq!(reader.len(), num_entries);

        // Verify we can read all entries
        let count = reader.iter().count();
        assert_eq!(count, num_entries);
    }

    #[test]
    fn test_index_block_builder_roundtrip() {
        use crate::format::IndexEntry;

        let mut builder = BlockBuilder::<IndexEntry>::new();

        // Create test index entries
        let index_entries = vec![
            IndexEntry::new(
                InternalKey::new(
                    UserKey::builder().key("key1").timestamp(100).build(),
                    3,
                    ValueType::Value,
                ),
                0,
                1024,
            ),
            IndexEntry::new(
                InternalKey::new(
                    UserKey::builder().key("key2").timestamp(200).build(),
                    2,
                    ValueType::Value,
                ),
                1024,
                2048,
            ),
            IndexEntry::new(
                InternalKey::new(
                    UserKey::builder().key("key3").timestamp(300).build(),
                    1,
                    ValueType::Value,
                ),
                3072,
                1536,
            ),
        ];

        // Add entries to the index block
        for entry in &index_entries {
            builder.add(entry.clone());
        }
        builder.finish();

        // Build the block
        let block_bytes = builder.build().expect("Failed to build index block");

        // Create reader from the bytes
        let cursor = Cursor::new(block_bytes);
        let mut reader = IndexBlockReader::new(cursor).expect("Failed to create reader");

        // Verify reader properties
        assert_eq!(reader.len(), index_entries.len());
        assert!(!reader.is_empty());

        // Read back all entries using iterator
        let read_entries: std::result::Result<Vec<_>, std::io::Error> = reader.iter().collect();
        let read_entries = read_entries.expect("Failed to read index entries");

        // Verify all entries match
        assert_eq!(read_entries.len(), index_entries.len());
        for (original, read) in index_entries.iter().zip(read_entries.iter()) {
            assert_eq!(original, read);
            // Verify the specific fields
            assert_eq!(original.key, read.key);
            assert_eq!(original.block_offset, read.block_offset);
            assert_eq!(original.block_size, read.block_size);
        }
    }

    #[test]
    fn test_type_aliases_work() {
        use crate::format::IndexEntry;

        // Test that our type aliases work correctly
        let mut data_builder = DataBlockBuilder::new();
        let data_entry = create_test_value("key1", 100, 1, "value1");
        data_builder.add(data_entry.clone());
        data_builder.finish();

        let mut index_builder = IndexBlockBuilder::new();
        let index_entry = IndexEntry::new(
            InternalKey::new(
                UserKey::builder().key("key1").timestamp(100).build(),
                1,
                ValueType::Value,
            ),
            0,
            1024,
        );
        index_builder.add(index_entry.clone());
        index_builder.finish();

        // Both should be able to build successfully
        assert!(data_builder.build().is_ok());
        assert!(index_builder.build().is_ok());
    }

    #[test]
    fn test_block_reader_get() {
        let mut builder = BlockBuilder::<Entry>::new();

        // Add test entries in sorted order
        let entries = vec![
            create_test_value("apple", 100, 5, "fruit"),
            create_test_value("banana", 200, 4, "yellow"),
            create_test_value("cherry", 150, 3, "red"),
            create_test_value("date", 300, 2, "sweet"),
            create_test_value("elderberry", 250, 1, "purple"),
        ];

        for entry in &entries {
            builder.add(entry.clone());
        }
        builder.finish();

        let block_bytes = builder.build().expect("Failed to build block");
        let cursor = Cursor::new(block_bytes);
        let mut reader = BlockReader::<Entry, _>::new(cursor).expect("Failed to create reader");

        // Test finding existing keys
        for entry in &entries {
            let found = reader.get(&entry.key).expect("Failed to search for key");
            assert!(found.is_some(), "Should find key: {:?}", entry.key);
            let found_entry = found.unwrap();
            assert_eq!(found_entry.key, entry.key);
            assert_eq!(found_entry.value, entry.value);
        }

        // Test key that doesn't exist but is within range
        let missing_key = create_test_value("grape", 200, 3, "missing").key;
        let found = reader
            .get(&missing_key)
            .expect("Failed to search for missing key");
        assert!(found.is_none(), "Should not find missing key");

        // Test key that's before all entries
        let before_key = create_test_value("aardvark", 100, 1, "before").key;
        let found = reader
            .get(&before_key)
            .expect("Failed to search for before key");
        assert!(found.is_none(), "Should not find key before range");

        // Test key that's after all entries
        let after_key = create_test_value("zebra", 100, 1, "after").key;
        let found = reader
            .get(&after_key)
            .expect("Failed to search for after key");
        assert!(found.is_none(), "Should not find key after range");
    }

    #[test]
    fn test_block_reader_get_at_index() {
        let mut builder = BlockBuilder::<Entry>::new();

        // Add test entries
        let entries = vec![
            create_test_value("key1", 100, 3, "value1"),
            create_test_value("key2", 200, 2, "value2"),
            create_test_value("key3", 300, 1, "value3"),
        ];

        for entry in &entries {
            builder.add(entry.clone());
        }
        builder.finish();

        let block_bytes = builder.build().expect("Failed to build block");
        let cursor = Cursor::new(block_bytes);
        let mut reader = BlockReader::<Entry, _>::new(cursor).expect("Failed to create reader");

        // Test getting entries by index
        for (i, expected_entry) in entries.iter().enumerate() {
            let found = reader
                .get_at_index(i)
                .expect("Failed to get entry at index");
            assert!(found.is_some(), "Should find entry at index {}", i);
            let found_entry = found.unwrap();
            assert_eq!(found_entry.key, expected_entry.key);
            assert_eq!(found_entry.value, expected_entry.value);
        }

        // Test out of bounds index
        let found = reader
            .get_at_index(entries.len())
            .expect("Failed to check out of bounds");
        assert!(
            found.is_none(),
            "Should not find entry at out of bounds index"
        );
    }

    #[test]
    fn test_block_reader_get_empty_block() {
        // Create an empty block (just entry count = 0)
        let mut block_bytes = Vec::new();
        block_bytes.extend_from_slice(&0u32.to_le_bytes());

        let cursor = Cursor::new(block_bytes);
        let mut reader = BlockReader::<Entry, _>::new(cursor).expect("Failed to create reader");

        // Test searching in empty block
        let test_key = create_test_value("test", 100, 1, "value").key;
        let found = reader
            .get(&test_key)
            .expect("Failed to search in empty block");
        assert!(found.is_none(), "Should not find anything in empty block");

        // Test get_at_index in empty block
        let found = reader
            .get_at_index(0)
            .expect("Failed to get at index in empty block");
        assert!(
            found.is_none(),
            "Should not find anything at index 0 in empty block"
        );
    }

    #[test_case(1; "single entry")]
    #[test_case(2; "two entries")]
    #[test_case(5; "small block")]
    #[test_case(10; "medium block")]
    #[test_case(100; "large block")]
    fn test_block_reader_get_stress(num_entries: usize) {
        let mut builder = BlockBuilder::<Entry>::new();

        // Create entries with predictable keys
        let mut entries = Vec::new();
        for i in 0..num_entries {
            let key = format!("key{:06}", i);
            let value = format!("value{}", i);
            let entry = create_test_value(&key, 100, i as u64, &value);
            entries.push(entry.clone());
            builder.add(entry);
        }
        builder.finish();

        let block_bytes = builder.build().expect("Failed to build block");
        let cursor = Cursor::new(block_bytes);
        let mut reader = BlockReader::<Entry, _>::new(cursor).expect("Failed to create reader");

        // Test finding all entries
        for entry in &entries {
            let found = reader.get(&entry.key).expect("Failed to search for key");
            assert!(found.is_some(), "Should find key: {:?}", entry.key);
            let found_entry = found.unwrap();
            assert_eq!(found_entry.key, entry.key);
            assert_eq!(found_entry.value, entry.value);
        }

        // Test get_at_index for all entries
        for (i, expected_entry) in entries.iter().enumerate() {
            let found = reader
                .get_at_index(i)
                .expect("Failed to get entry at index");
            assert!(found.is_some(), "Should find entry at index {}", i);
            let found_entry = found.unwrap();
            assert_eq!(found_entry.key, expected_entry.key);
            assert_eq!(found_entry.value, expected_entry.value);
        }
    }
}
