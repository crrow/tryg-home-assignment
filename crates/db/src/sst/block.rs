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

use std::io::{SeekFrom, Write};

use crate::format::{Codec, InternalKey, InternalValue};

/// A block builder is used to build a block in SSTable format.
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
pub(crate) struct BlockBuilder {
    /// The internal values stored in this block
    items:          Vec<InternalValue>,
    /// Byte offsets for each item in the data section
    offsets:        Vec<u64>,
    /// Current offset in the data section
    current_offset: u64,
    /// The last key in the block
    last_key:       Option<InternalKey>,
    /// Whether this builder has been finished and can no longer accept new
    /// items
    finished:       bool,
}

impl BlockBuilder {
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

    /// Adds a new internal value to the block.
    ///
    /// Returns an error if the block builder has already been finished.
    pub(crate) fn add(&mut self, value: InternalValue) {
        assert!(!self.finished, "BlockBuilder should not be finished");
        // Ensure keys are added in sorted order
        if let Some(ref last_key) = self.last_key {
            assert!(
                value.key > *last_key,
                "Keys must be added in sorted order: new key {:?} is not greater than last key \
                 {:?}",
                value.key,
                last_key
            );
        }

        self.offsets.push(self.current_offset);
        self.current_offset += value.size();
        self.last_key = Some(value.key.clone());
        self.items.push(value);
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
    ///
    /// Returns an error if the block is empty.
    pub(crate) fn finish(&mut self) { self.finished = true; }

    /// Returns whether the block builder has been finished.
    pub(crate) fn is_finished(&self) -> bool { self.finished }

    /// Clears all items from the block builder and resets it to an unfinished
    /// state.
    pub(crate) fn clear(&mut self) {
        self.items.clear();
        self.offsets.clear();
        self.current_offset = 0;
        self.finished = false;
    }

    /// Returns an iterator over the items in the block.
    pub(crate) fn items(&self) -> impl Iterator<Item = &InternalValue> { self.items.iter() }

    /// Returns a slice of the offsets for each item.
    pub(crate) fn offsets(&self) -> &[u64] { &self.offsets }

    /// Writes the block to the provided writer.
    ///
    /// The block format is:
    /// - Data section: serialized InternalValues
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

        // Write data section - serialize each InternalValue
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

/// A reader for reading a block from a reader.
pub(crate) struct BlockReader<R: std::io::Read> {
    reader:  R,
    offsets: Vec<u64>,
    count:   u32,
}

impl<R: std::io::Read + std::io::Seek> BlockReader<R> {
    pub(crate) fn new(mut reader: R) -> std::io::Result<Self> {
        // Try to read from the end to get the entry count
        let mut buf = [0; 4];
        reader.seek(SeekFrom::End(-4))?;
        reader.read_exact(&mut buf)?;

        let count = u32::from_le_bytes(buf);
        let offset_section_size = count as usize * std::mem::size_of::<u64>();
        let mut offset_buf = vec![0; offset_section_size];
        // Read offset table from the position before the entry count
        reader.seek(SeekFrom::End(-4 - offset_section_size as i64))?;
        reader.read_exact(&mut offset_buf)?;
        let offsets = offset_buf
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        Ok(Self {
            reader,
            offsets,
            count,
        })
    }

    /// Returns an iterator over the InternalValues in this block.
    pub(crate) fn iter(&mut self) -> BlockIterator<'_, R> {
        BlockIterator {
            reader:        &mut self.reader,
            offsets:       &self.offsets,
            current_index: 0,
            count:         self.count,
        }
    }

    /// Returns the number of entries in this block.
    pub(crate) fn len(&self) -> usize { self.count as usize }

    /// Returns true if the block is empty.
    pub(crate) fn is_empty(&self) -> bool { self.count == 0 }
}

/// Iterator over the entries in a block.
pub(crate) struct BlockIterator<'a, R: std::io::Read + std::io::Seek> {
    reader:        &'a mut R,
    offsets:       &'a [u64],
    current_index: usize,
    count:         u32,
}

impl<'a, R: std::io::Read + std::io::Seek> Iterator for BlockIterator<'a, R> {
    type Item = std::io::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.count as usize {
            return None;
        }

        let offset = self.offsets[self.current_index];

        // Seek to the position of the current entry
        if let Err(e) = self.reader.seek(SeekFrom::Start(offset)) {
            return Some(Err(e));
        }

        // Decode the InternalValue from the reader
        match InternalValue::decode_from(self.reader) {
            Ok(value) => {
                self.current_index += 1;
                Some(Ok(value))
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count as usize - self.current_index;
        (remaining, Some(remaining))
    }
}

impl<'a, R: std::io::Read + std::io::Seek> ExactSizeIterator for BlockIterator<'a, R> {}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use test_case::test_case;

    use super::*;
    use crate::format::{InternalValue, UserKey, ValueType};

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
    fn create_test_value(key: &str, timestamp: u64, seqno: u64, value: &str) -> InternalValue {
        InternalValue::make(key, timestamp, seqno, value)
    }

    /// Helper function to create tombstone InternalValue
    fn create_tombstone(key: &str, timestamp: u64, seqno: u64) -> InternalValue {
        InternalValue {
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
        let builder = BlockBuilder::new();
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
        let mut builder = BlockBuilder::new();
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
        let mut builder = BlockBuilder::new();
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
        let mut builder = BlockBuilder::new();
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
        let mut builder = BlockBuilder::new();
        let value1 = create_test_value(first_key, first_timestamp, first_seqno, "value1");
        let value2 = create_test_value(second_key, second_timestamp, second_seqno, "value2");

        builder.add(value1);
        builder.add(value2); // Should succeed for all test cases

        assert_eq!(builder.len(), 2);
    }

    #[test]
    #[should_panic(expected = "BlockBuilder should not be finished")]
    fn test_block_builder_prevents_add_after_finish() {
        let mut builder = BlockBuilder::new();
        let value = create_test_value("key1", 100, 1, "value1");

        builder.add(value.clone());
        builder.finish();
        builder.add(value); // Should panic
    }

    #[test]
    fn test_block_builder_estimate_size() {
        let mut builder = BlockBuilder::new();
        let initial_size = builder.estimate_size();

        let value1 = create_test_value("key1", 100, 1, "value1");
        builder.add(value1.clone());

        let expected_size = initial_size + value1.size() + 8; // +8 for offset
        assert_eq!(builder.estimate_size(), expected_size);
    }

    #[test]
    fn test_block_builder_clear() {
        let mut builder = BlockBuilder::new();
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
        let mut builder = BlockBuilder::new();

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
        let mut builder = BlockBuilder::new();

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
        let mut reader = BlockReader::new(cursor).expect("Failed to create reader");

        let read_values: Result<Vec<_>, _> = reader.iter().collect();
        let read_values = read_values.expect("Failed to read values");

        assert_eq!(read_values.len(), values.len());
        for (original, read) in values.iter().zip(read_values.iter()) {
            assert_eq!(original, read);
        }
    }

    #[test]
    fn test_block_builder_and_reader_roundtrip() {
        let mut builder = BlockBuilder::new();

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
        let mut reader = BlockReader::new(cursor).expect("Failed to create reader");

        // Verify reader properties
        assert_eq!(reader.len(), values.len());
        assert!(!reader.is_empty());

        // Read back all values using iterator
        let read_values: Result<Vec<_>, _> = reader.iter().collect();
        let read_values = read_values.expect("Failed to read values");

        // Verify all values match
        assert_eq!(read_values.len(), values.len());
        for (original, read) in values.iter().zip(read_values.iter()) {
            assert_eq!(original, read);
        }
    }

    #[test]
    fn test_build_into_memory_efficiency() {
        let mut builder = BlockBuilder::new();

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
        let mut builder = BlockBuilder::new();

        // Add test data
        for i in 0..num_items {
            let value = create_test_value(&format!("key{i}"), 100, i as u64, "value");
            builder.add(value);
        }

        if num_items > 0 {
            builder.finish();
            let block_bytes = builder.build().expect("Failed to build block");
            let cursor = Cursor::new(block_bytes);
            let mut reader = BlockReader::new(cursor).expect("Failed to create reader");

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
            let mut reader = BlockReader::new(cursor).expect("Failed to create reader");
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
        let mut reader = BlockReader::new(cursor).expect("Failed to create reader");

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
        let mut builder = BlockBuilder::new();

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
        let mut reader = BlockReader::new(cursor).expect("Failed to create reader");

        let values: Result<Vec<_>, _> = reader.iter().collect();
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
        let mut builder = BlockBuilder::new();

        // Add entries to test offset calculations
        for i in 0..num_entries {
            let key = format!("key{i:06}");
            let value = format!("value{i}");
            builder.add(create_test_value(&key, 100, i as u64, &value));
        }

        builder.finish();
        let block_bytes = builder.build().expect("Failed to build block");

        let cursor = Cursor::new(block_bytes);
        let mut reader = BlockReader::new(cursor).expect("Failed to create reader");

        assert_eq!(reader.len(), num_entries);

        // Verify we can read all entries
        let count = reader.iter().count();
        assert_eq!(count, num_entries);
    }
}
