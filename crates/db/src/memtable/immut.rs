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

use crate::memtable::MemTable;

pub(crate) type MemTableID = u64;

use std::sync::{Arc, RwLock, RwLockReadGuard};

/// Stores references to all sealed memtables in a thread-safe manner.
///
/// Memtable IDs are monotonically increasing, so we don't really
/// need a search tree; also there are only a handful of them at most.
/// Internally uses a RwLock to allow concurrent reads and exclusive writes.
#[derive(Debug)]
pub(crate) struct ImmutableMemTables {
    tables: RwLock<Vec<(MemTableID, Arc<MemTable>)>>,
}

impl ImmutableMemTables {
    /// Creates a new, empty collection of immutable memtables.
    pub(crate) fn new() -> Self {
        ImmutableMemTables {
            tables: RwLock::new(Vec::new()),
        }
    }

    /// Adds a new immutable memtable with the given ID.
    /// Acquires a write lock.
    pub(crate) fn add(&self, id: MemTableID, memtable: Arc<MemTable>) {
        let mut tables = self.tables.write().unwrap();
        tables.push((id, memtable));
    }

    /// Returns a read guard to the internal vector of (MemTableID,
    /// Arc<MemTable>) pairs. The caller can iterate over the guard as
    /// needed.
    pub(crate) fn read_tables(&self) -> RwLockReadGuard<'_, Vec<(MemTableID, Arc<MemTable>)>> {
        self.tables.read().unwrap()
    }

    /// Returns the number of immutable memtables.
    pub(crate) fn len(&self) -> usize {
        let tables = self.tables.read().unwrap();
        tables.len()
    }

    /// Returns true if there are no immutable memtables.
    pub(crate) fn is_empty(&self) -> bool {
        let tables = self.tables.read().unwrap();
        tables.is_empty()
    }

    /// Gets a clone of the Arc<MemTable> with the given ID, if it exists.
    /// This avoids holding the lock after the call.
    pub(crate) fn get(&self, id: MemTableID) -> Option<Arc<MemTable>> {
        let tables = self.tables.read().unwrap();
        tables
            .iter()
            .find(|(table_id, _)| *table_id == id)
            .map(|(_, memtable)| Arc::clone(memtable))
    }

    /// Removes and returns the memtable with the given ID, if it exists.
    /// Acquires a write lock.
    pub(crate) fn remove(&self, id: MemTableID) -> Option<Arc<MemTable>> {
        let mut tables = self.tables.write().unwrap();
        tables
            .iter()
            .position(|(table_id, _)| *table_id == id)
            .map(|pos| tables.remove(pos).1)
    }
}
