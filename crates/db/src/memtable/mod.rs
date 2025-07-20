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

use std::sync::atomic::{AtomicU64, Ordering};

use bon::{Builder, builder};
use crossbeam_skiplist::SkipMap;

use crate::format::{InternalKey, InternalValue, SeqNo, Timestamp, UserKey, UserValue};

#[derive(Debug, Builder)]
pub(crate) struct MemTable {
    // The series in this memtable, keyed by series_ref.
    #[builder(default = SkipMap::new())]
    items:            SkipMap<InternalKey, UserValue>,
    // The approximate size of memtable in bytes.
    #[builder(default = AtomicU64::new(0))]
    approximate_size: AtomicU64,
    /// seqno is used for mvcc.
    #[builder(default = AtomicU64::new(0))]
    highest_seqno:    AtomicU64,
}

impl MemTable {
    /// Appends a data point to the memtable.
    ///
    /// Returns the new size of the memtable.
    pub(crate) fn insert(&self, item: InternalValue) -> u64 {
        let item_size = item.size();
        let size_before = self.approximate_size.fetch_add(item_size, Ordering::AcqRel);

        self.items.insert(item.key.clone(), item.value);
        self.highest_seqno
            .fetch_max(item.key.seqno, std::sync::atomic::Ordering::AcqRel);
        size_before + item_size
    }

    /// Creates an iterator over all items.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = InternalValue> + '_ {
        self.items.iter().map(|entry| InternalValue {
            key:   entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    /// Returns the most recent value for the given key and timestamp, not
    /// exceeding the given seqno (if provided).
    pub(crate) fn get(&self, key: &[u8], ts: Timestamp, seqno: SeqNo) -> Option<InternalValue> {
        // We want the latest value for (key, ts) <= given ts, and seqno <= given seqno.
        // Instead of trying to construct a precise search key, we'll search for all
        // entries with the given key and filter by timestamp and seqno
        // constraints.

        // Create a range that includes all possible entries for this key.
        // Since UserKey orders by key ascending then timestamp descending,
        // and InternalKey orders by UserKey then seqno descending,
        // we need to reverse the order for the range.
        let start_key = InternalKey::builder()
            .user_key(UserKey::builder().key(key).timestamp(u64::MAX).build())
            .seqno(u64::MAX)
            .build();

        let end_key = InternalKey::builder()
            .user_key(UserKey::builder().key(key).timestamp(0).build())
            .seqno(0)
            .build();

        // Get all entries for this key
        let iter = self.items.range(start_key..=end_key);

        let mut best_match: Option<InternalValue> = None;

        for entry in iter {
            let found_key = entry.key();

            // Check that the found key matches the user key
            if found_key.user_key.key.as_ref() == key {
                // Check timestamp and seqno constraints
                if found_key.user_key.timestamp <= ts && found_key.seqno <= seqno {
                    // If it's a tombstone, we're done for this user key
                    if found_key.is_tombstone() {
                        return None;
                    }

                    // Check if this is a better match than what we have
                    let is_better = match &best_match {
                        None => true,
                        Some(current_best) => {
                            // We want the entry with the highest seqno (most recent write), and if
                            // seqnos are equal, the highest timestamp
                            // (most recent data)
                            found_key.seqno > current_best.key.seqno
                                || (found_key.seqno == current_best.key.seqno
                                    && found_key.user_key.timestamp
                                        > current_best.key.user_key.timestamp)
                        }
                    };

                    if is_better {
                        best_match = Some(InternalValue {
                            key:   found_key.clone(),
                            value: entry.value().clone(),
                        });
                    }
                }
            }
        }

        best_match
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    /// Table-driven test for MemTable basic insert and get operations.
    /// Each test case specifies the key, value, timestamp, seqno, and the query
    /// constraints.
    #[test_case(
        // Insert one entry, get it back
        vec![("key1", "value1", 100, 1)],
        "key1", 100, 1,
        Some(("key1", "value1", 100, 1));
        "simple insert and get"
    )]
    #[test_case(
        // Insert one entry, query non-existent key
        vec![("key1", "value1", 100, 1)],
        "non-existent", 100, 1,
        None;
        "get non-existent key"
    )]
    #[test_case(
        // Insert two versions, get latest
        vec![
            ("key1", "value1", 100, 1),
            ("key1", "value2", 200, 2)
        ],
        "key1", 200, 2,
        Some(("key1", "value2", 200, 2));
        "get latest version"
    )]
    #[test_case(
        // Insert two versions, get older by specifying timestamp and seqno
        vec![
            ("key1", "value1", 100, 1),
            ("key1", "value2", 200, 2)
        ],
        "key1", 100, 1,
        Some(("key1", "value1", 100, 1));
        "get older version"
    )]
    #[test_case(
        // Insert two versions, get with timestamp between versions (should return older)
        vec![
            ("key1", "value1", 100, 1),
            ("key1", "value2", 200, 2)
        ],
        "key1", 150, 2,
        Some(("key1", "value1", 100, 1));
        "get with timestamp between versions"
    )]
    #[test_case(
        // Insert two versions, get with higher seqno (should return latest)
        vec![
            ("key1", "value1", 100, 1),
            ("key1", "value2", 200, 2)
        ],
        "key1", 200, 12,
        Some(("key1", "value2", 200, 2));
        "get with higher seqno"
    )]
    fn memtable(
        inserts: Vec<(&str, &str, u64, u64)>,
        query_key: &str,
        query_ts: u64,
        query_seqno: u64,
        expected: Option<(&str, &str, u64, u64)>,
    ) {
        let memtable = MemTable::builder().build();

        // Insert all test entries
        for (k, v, ts, seq) in &inserts {
            let key = k.as_bytes();
            let value = v.as_bytes();
            let internal_value = InternalValue::make(key, *ts, *seq, value);
            memtable.insert(internal_value);
        }

        let result = memtable.get(query_key.as_bytes(), query_ts, query_seqno);

        match expected {
            Some((exp_key, exp_value, exp_ts, exp_seq)) => {
                let value = result.expect("Expected a value but got None");
                assert_eq!(value.key.user_key.key.as_ref(), exp_key.as_bytes());
                assert_eq!(value.value.as_ref(), exp_value.as_bytes());
                assert_eq!(value.key.user_key.timestamp, exp_ts);
                assert_eq!(value.key.seqno, exp_seq);
            }
            None => {
                assert!(result.is_none(), "Expected None but got Some value");
            }
        }
    }

    /// Helper function to insert a fixed set of test entries into the memtable.
    /// This ensures all test cases operate on the same data set.
    fn setup_memtable_with_versions() -> MemTable {
        let memtable = MemTable::builder().build();
        let key = "sensor_data".as_bytes();

        // Insert multiple versions of the same key with different timestamps and
        // seqnos. Entry 1: ts=100, seqno=1
        let entry1 = InternalValue::make(key, 100, 1, "value_at_100_seq1".as_bytes());
        memtable.insert(entry1);

        // Entry 2: ts=200, seqno=2
        let entry2 = InternalValue::make(key, 200, 2, "value_at_200_seq2".as_bytes());
        memtable.insert(entry2);

        // Entry 3: ts=150, seqno=3 (newer seqno but older timestamp than entry2)
        let entry3 = InternalValue::make(key, 150, 3, "value_at_150_seq3".as_bytes());
        memtable.insert(entry3);

        // Entry 4: ts=200, seqno=4 (same timestamp as entry2 but newer seqno)
        let entry4 = InternalValue::make(key, 200, 4, "value_at_200_seq4".as_bytes());
        memtable.insert(entry4);

        memtable
    }

    /// Table-driven test for MemTable::get using the test-case crate.
    /// Each test case specifies the query constraints and the expected result.
    #[test_case(300, 10, Some((200, 4)); "latest version with high ts and seqno")]
    #[test_case(300, 3, Some((150, 3)); "seqno constraint excludes latest")]
    #[test_case(175, 10, Some((150, 3)); "timestamp constraint excludes ts=200")]
    #[test_case(125, 2, Some((100, 1)); "tight constraints, only oldest valid entry")]
    #[test_case(50, 1, None; "no entry satisfies constraints")]
    fn test_better_match_logic(ts: u64, seqno: u64, expected: Option<(u64, u64)>) {
        let memtable = setup_memtable_with_versions();
        let key = "sensor_data".as_bytes();

        let result = memtable.get(key, ts, seqno);

        match expected {
            Some((exp_ts, exp_seqno)) => {
                let value = result.expect("Expected a value but got None");
                assert_eq!(
                    value.key.user_key.timestamp, exp_ts,
                    "Expected timestamp {}, got {}",
                    exp_ts, value.key.user_key.timestamp
                );
                assert_eq!(
                    value.key.seqno, exp_seqno,
                    "Expected seqno {}, got {}",
                    exp_seqno, value.key.seqno
                );
            }
            None => {
                assert!(result.is_none(), "Expected None, but got Some({result:?})");
            }
        }
    }
}
