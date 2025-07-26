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

use std::io::{Read, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::format::{Codec, Entry, InternalKey, SeqNo, Timestamp, UserKey, UserValue, ValueType};

/// `WriteBatch` holds a collection of InternalValue updates to apply atomically
/// to a DB.
///
/// ```text
/// The serialized format:
///
///  +---------------------+
///  | sequence number (8) |  the starting seq number
///  +---------------------+
///  | count (4)           |  number of operations
///  +---------------------+
///  | internal values...  |  variable length InternalValue entries
///  +---------------------+
///
/// Each InternalValue is serialized using the Codec trait.
/// ```
/// The updates are applied in the order in which they are added
/// to the `WriteBatch`.
#[derive(Debug, Clone)]
pub struct WriteBatch {
    /// Starting sequence number for this batch
    sequence: SeqNo,
    /// List of InternalValue operations in this batch
    values:   Vec<Entry>,
}

pub const HEADER_SIZE: usize = 12; // 8 bytes seq + 4 bytes count

impl WriteBatch {
    /// Creates a new empty WriteBatch with the given sequence number
    pub fn new(sequence: SeqNo) -> Self {
        Self {
            sequence,
            values: Vec::new(),
        }
    }

    /// Adds a Put operation to the batch
    pub fn put(&mut self, key: &[u8], value: &[u8], timestamp: Timestamp) {
        let user_key = UserKey {
            key: key.into(),
            timestamp,
        };

        let internal_key = InternalKey {
            user_key,
            seqno: self.sequence, // All operations in batch use same sequence
            value_type: ValueType::Value,
        };

        let internal_value = Entry {
            key:   internal_key,
            value: value.into(),
        };

        self.values.push(internal_value);
    }

    /// Adds a Delete operation to the batch
    pub fn delete(&mut self, key: &[u8], timestamp: Timestamp) {
        let user_key = UserKey {
            key: key.into(),
            timestamp,
        };

        let internal_key = InternalKey {
            user_key,
            seqno: self.sequence, // All operations in batch use same sequence
            value_type: ValueType::Tombstone,
        };

        let internal_value = Entry {
            key:   internal_key,
            value: UserValue::from(""), // Empty value for tombstones
        };

        self.values.push(internal_value);
    }

    /// Returns the number of operations in this batch
    pub fn count(&self) -> u32 { self.values.len() as u32 }

    /// Returns true if the batch is empty
    pub fn is_empty(&self) -> bool { self.values.is_empty() }

    /// Clears all operations from the batch
    pub fn clear(&mut self) { self.values.clear(); }

    /// Returns the header size in bytes
    pub(crate) fn header_size(&self) -> usize { HEADER_SIZE }

    pub(crate) fn approximate_size(&self) -> usize {
        let mut size = HEADER_SIZE;
        for value in &self.values {
            size += value.size() as usize;
        }
        size
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Entry> { self.values.iter() }

    pub(crate) fn encode_header(&self) -> std::io::Result<Vec<u8>> {
        // Pre-allocate buffer with the expected header size for efficiency.
        let mut buf = Vec::with_capacity(HEADER_SIZE);
        buf.write_u64::<LittleEndian>(self.sequence)?;
        buf.write_u32::<LittleEndian>(self.count())?;
        Ok(buf)
    }
}

impl Default for WriteBatch {
    fn default() -> Self { Self::new(0) }
}

impl Codec for WriteBatch {
    fn encode_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        // Write header: sequence number and count
        writer.write_u64::<LittleEndian>(self.sequence)?;
        writer.write_u32::<LittleEndian>(self.count())?;

        // Write each InternalValue
        for value in &self.values {
            value.encode_into(writer)?;
        }
        Ok(())
    }

    fn decode_from<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let sequence = reader.read_u64::<LittleEndian>()?;
        let count = reader.read_u32::<LittleEndian>()?;

        let mut values = Vec::with_capacity(count as usize);

        // Read each InternalValue
        for _ in 0..count {
            let value = Entry::decode_from(reader)?;
            values.push(value);
        }

        Ok(Self { sequence, values })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_batch_basic() {
        let mut batch = WriteBatch::new(100);

        // Add some operations
        batch.put(b"key1", b"value1", 1000);
        batch.put(b"key2", b"value2", 2000);
        batch.delete(b"key3", 3000);

        assert!(!batch.is_empty());
        assert_eq!(batch.count(), 3);

        // Test sequence numbers are correctly assigned
        let values = batch.values;
        assert_eq!(values[0].key.seqno, 100);
        assert_eq!(values[1].key.seqno, 100);
        assert_eq!(values[2].key.seqno, 100);

        // Test value types
        assert_eq!(values[0].key.value_type, ValueType::Value);
        assert_eq!(values[1].key.value_type, ValueType::Value);
        assert_eq!(values[2].key.value_type, ValueType::Tombstone);
    }

    #[test]
    fn test_write_batch_encode_decode() {
        let mut batch = WriteBatch::new(42);
        batch.put(b"hello", b"world", 1234);
        batch.delete(b"goodbye", 5678);

        // Encode the batch
        let mut encoded = vec![];
        batch.encode_into(&mut encoded).unwrap();

        // Decode it back
        let decoded = WriteBatch::decode_from(&mut encoded.as_slice()).unwrap();

        let orig_values = batch.values;
        let decoded_values = decoded.values;

        for (orig, decoded) in orig_values.iter().zip(decoded_values.iter()) {
            assert_eq!(orig.key.user_key.key, decoded.key.user_key.key);
            assert_eq!(orig.key.user_key.timestamp, decoded.key.user_key.timestamp);
            assert_eq!(orig.key.seqno, decoded.key.seqno);
            assert_eq!(orig.key.value_type, decoded.key.value_type);
            assert_eq!(orig.value, decoded.value);
        }
    }
}
