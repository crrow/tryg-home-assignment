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

use fusio::fs::Fs;
use rsketch_common::readable_size::ReadableSize;

use crate::{
    err::Result,
    format::{Entry, InternalKey},
    sst::block::{DataBlockBuilder, IndexBlockBuilder},
};

pub(crate) struct TableBuilder<FS: Fs> {
    // the underlying file to write to
    file:                 FS::File,
    data_block:           DataBlockBuilder,
    index_block:          IndexBlockBuilder,
    last_key:             Option<InternalKey>,
    closed:               bool,
    num_entries:          u32,
    block_size_threshold: ReadableSize,
}

impl<FS: Fs> TableBuilder<FS> {
    pub(crate) fn new(file: FS::File) -> Self {
        Self {
            file,
            data_block: DataBlockBuilder::new(),
            index_block: IndexBlockBuilder::new(),
            last_key: None,
            closed: false,
            num_entries: 0,
            block_size_threshold: ReadableSize::mb(1),
        }
    }

    pub(crate) fn add(&mut self, entry: Entry) -> Result<()> {
        assert!(!self.closed);
        if let Some(last_key) = &self.last_key {
            assert!(last_key < &entry.key, "key is not in order");
        }

        self.last_key = Some(entry.key.clone());
        self.num_entries += 1;
        self.data_block.add(entry);
        if self.data_block.estimate_size() >= self.block_size_threshold.as_bytes() {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> { todo!() }
}
