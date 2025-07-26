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
use tokio::io::AsyncRead;
use value_log::Slice;

pub(crate) const MANIFEST_FILE: &str = "manifest";

/// Trait to serialize stuff
pub trait Codec {
    /// Serializes into writer.
    fn encode_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()>;

    /// Serializes into vector.
    #[allow(unused)]
    fn encode_into_vec(&self) -> std::io::Result<Vec<u8>> {
        let mut v = vec![];
        self.encode_into(&mut v)?;
        Ok(v)
    }

    /// Deserializes from reader.
    fn decode_from<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized;
}
/// Sequence number.
pub type SeqNo = u64;
pub type Timestamp = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash, bon::Builder)]
pub struct UserKey {
    #[builder(into)]
    pub key:       Slice,
    pub timestamp: Timestamp,
}

impl Ord for UserKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Order by key ascending, then by timestamp descending.
        // This makes it easy to find the latest version of a key.
        self.key
            .cmp(&other.key)
            .then_with(|| other.timestamp.cmp(&self.timestamp))
    }
}

impl PartialOrd for UserKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

/// Value type (regular value or tombstone)
#[derive(Clone, PartialEq, Eq, Hash, bon::Builder)]
pub struct InternalKey {
    #[builder(into)]
    pub user_key:   UserKey,
    pub seqno:      SeqNo,
    #[builder(default = ValueType::Value)]
    pub value_type: ValueType,
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

/// Compare by UserKey, then by sequence number.
impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.seqno.cmp(&self.seqno))
    }
}

impl std::fmt::Debug for InternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{}",
            str::from_utf8(self.user_key.key.as_ref()).unwrap(),
            self.seqno,
            match self.value_type {
                ValueType::Value => "V",
                ValueType::Tombstone => "T",
            },
        )
    }
}

impl Codec for InternalKey {
    fn encode_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.seqno)?;
        writer.write_u8(self.value_type as u8)?;
        writer.write_u64::<LittleEndian>(self.user_key.timestamp)?;
        let key_len = self.user_key.key.len() as u64;
        writer.write_u64::<LittleEndian>(key_len)?;
        writer.write_all(&self.user_key.key)?;
        Ok(())
    }

    fn decode_from<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let seqno = reader.read_u64::<LittleEndian>()?;
        let value_type = ValueType::from(reader.read_u8()?);
        let timestamp = reader.read_u64::<LittleEndian>()?;
        let key_len = reader.read_u64::<LittleEndian>()?;
        let mut key = vec![0; key_len as usize];
        reader.read_exact(&mut key)?;
        Ok(Self {
            user_key: UserKey {
                key: Slice::from(key),
                timestamp,
            },
            seqno,
            value_type,
        })
    }
}

impl InternalKey {
    pub fn is_tombstone(&self) -> bool { self.value_type == ValueType::Tombstone }

    /// Returns the serialized size of this InternalKey in bytes.
    /// This includes:
    /// - 8 bytes for seqno (u64)
    /// - 1 byte for value_type (u8)
    /// - 8 bytes for timestamp (u64)
    /// - 8 bytes for key length (u64)
    /// - N bytes for the key itself
    pub(crate) fn size(&self) -> u64 {
        8  // seqno: u64
        + 1  // value_type: u8
        + 8  // timestamp: u64
        + 8  // key length: u64
        + self.user_key.key.len() as u64 // key bytes
    }

    pub fn new<K: Into<UserKey>>(user_key: K, seqno: SeqNo, value_type: ValueType) -> Self {
        let user_key = user_key.into();

        assert!(
            user_key.key.len() <= u16::MAX.into(),
            "keys can be 65535 bytes in length",
        );

        Self {
            user_key,
            seqno,
            value_type,
        }
    }

    pub async fn async_decode_from<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Self> {
        use tokio::io::AsyncReadExt;

        let seqno = reader.read_u64_le().await?;
        let value_type = ValueType::from(reader.read_u8().await?);
        let timestamp = reader.read_u64_le().await?;
        let key_len = reader.read_u64_le().await?;
        let mut key = vec![0; key_len as usize];
        reader.read_exact(&mut key).await?;

        Ok(Self {
            user_key: UserKey {
                key: Slice::from(key),
                timestamp,
            },
            seqno,
            value_type,
        })
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum ValueType {
    /// Existing value
    Value = 0,

    /// Deleted value
    Tombstone = 1,
}

impl From<u8> for ValueType {
    fn from(value: u8) -> Self {
        match value {
            0 => ValueType::Value,
            1 => ValueType::Tombstone,
            _ => panic!("Invalid value type: {value}"),
        }
    }
}

/// User defined data (blob of bytes).
pub type UserValue = Slice;

#[derive(Clone, PartialEq, Eq)]
pub struct Entry {
    /// An internal key.
    pub key:   InternalKey,
    /// User-defined value - an arbitrary byte array
    ///
    /// Supports up to 2^32 bytes
    pub value: UserValue,
}

impl Entry {
    /// Returns the serialized size of this InternalValue in bytes.
    /// This includes:
    /// - the serialized size of the key,
    /// - 8 bytes for the value length (u64),
    /// - the length of the value itself.
    pub fn size(&self) -> u64 { self.key.size() + 8 + self.value.len() as u64 }

    /// Constructs an InternalValue from a key, timestamp, sequence number, and
    /// value. Accepts both `&[u8]` and `&str` for the value parameter.
    pub(crate) fn make<K, V>(key: K, ts: Timestamp, seqno: SeqNo, value: V) -> Self
    where
        K: AsRef<[u8]>,
        V: Into<Slice>,
    {
        Self {
            key:   InternalKey::new(
                UserKey::builder().key(key.as_ref()).timestamp(ts).build(),
                seqno,
                ValueType::Value,
            ),
            value: value.into(),
        }
    }

    pub(crate) async fn async_decode_from<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        use tokio::io::AsyncReadExt;

        let key = InternalKey::async_decode_from(reader).await?;
        let value_len = reader.read_u64_le().await?;
        let mut value = vec![0; value_len as usize];
        reader.read_exact(&mut value).await?;

        Ok(Self {
            key,
            value: Slice::from(value),
        })
    }
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{:?}",
            self.key,
            str::from_utf8(self.value.as_ref()).unwrap()
        )
    }
}

impl Codec for Entry {
    fn encode_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.key.encode_into(writer)?;
        writer.write_u64::<LittleEndian>(self.value.len() as u64)?;
        writer.write_all(&self.value)?;
        Ok(())
    }

    fn decode_from<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let key = InternalKey::decode_from(reader)?;
        let value_len = reader.read_u64::<LittleEndian>()?;
        let mut value = vec![0; value_len as usize];
        reader.read_exact(&mut value)?;
        Ok(Self {
            key,
            value: Slice::from(value),
        })
    }
}

/// An entry in an index block that maps a key to a data block location.
///
/// Index entries are used to quickly locate which data block contains
/// a particular key range. Each index entry contains:
/// - The last key in a data block
/// - The file offset where that data block starts
/// - The size of the data block in bytes
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IndexEntry {
    /// The last key in the data block this entry points to
    pub key:          InternalKey,
    /// File offset where the data block starts
    pub block_offset: u64,
    /// Size of the data block in bytes
    pub block_size:   u64,
}

impl IndexEntry {
    /// Creates a new index entry.
    pub fn new(key: InternalKey, block_offset: u64, block_size: u64) -> Self {
        Self {
            key,
            block_offset,
            block_size,
        }
    }
}

impl Codec for IndexEntry {
    fn encode_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        // Encode the key first
        self.key.encode_into(writer)?;
        // Then the block offset and size
        writer.write_u64::<LittleEndian>(self.block_offset)?;
        writer.write_u64::<LittleEndian>(self.block_size)?;
        Ok(())
    }

    fn decode_from<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        // Decode the key first
        let key = InternalKey::decode_from(reader)?;
        // Then the block offset and size
        let block_offset = reader.read_u64::<LittleEndian>()?;
        let block_size = reader.read_u64::<LittleEndian>()?;

        Ok(Self {
            key,
            block_offset,
            block_size,
        })
    }
}
