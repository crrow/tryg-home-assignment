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
use value_log::Slice;

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

#[derive(Debug, Clone, PartialEq, Eq, bon::Builder)]
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
#[derive(Clone, PartialEq, Eq, bon::Builder)]
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

    pub fn size(&self) -> u64 {
        self.user_key.key.len() as u64
            + std::mem::size_of::<u64>() as u64
            + std::mem::size_of::<u8>() as u64
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
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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
pub struct InternalValue {
    /// An internal key.
    pub key:   InternalKey,
    /// User-defined value - an arbitrary byte array
    ///
    /// Supports up to 2^32 bytes
    pub value: UserValue,
}

impl InternalValue {
    pub fn size(&self) -> u64 { self.key.size() + self.value.len() as u64 }

    pub(crate) fn make(key: &[u8], ts: Timestamp, seqno: SeqNo, value: &[u8]) -> Self {
        Self {
            key:   InternalKey::new(
                UserKey::builder().key(key).timestamp(ts).build(),
                seqno,
                ValueType::Value,
            ),
            value: Slice::from(value),
        }
    }
}

impl std::fmt::Debug for InternalValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{:?}",
            self.key,
            str::from_utf8(self.value.as_ref()).unwrap()
        )
    }
}
