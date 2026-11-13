//! Adjacency list containing an integer sequence and a bitmap with rank and select support.
use crate::containers::{Bitmap, Sequence};
use crate::triples::Id;
use std::cmp::Ordering;

/// Adjacency list including a compact integer sequence and a bitmap for efficient access of that sequence using rank and select queries.
#[derive(Debug)]
pub struct AdjList {
    /// Compact integer sequence.
    pub sequence: Sequence,
    /// Helper structure for rank and select queries.
    pub bitmap: Bitmap,
}

impl AdjList {
    /// Adjacency list with the given sequence and bitmap.
    pub const fn new(sequence: Sequence, bitmap: Bitmap) -> Self {
        AdjList { sequence, bitmap }
    }

    /// Combined size in bytes of the sequence and the bitmap on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.sequence.size_in_bytes() + self.bitmap.size_in_bytes()
    }

    /// Whether the given position represents the last child of the parent node.
    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        self.bitmap.at_last_sibling(word_index)
    }

    /// Get the ID at the given position.
    pub fn get_id(&self, word_index: usize) -> Id {
        self.sequence.get(word_index) as Id
    }

    /// Number of entries in both the integer sequence and the bitmap.
    pub const fn len(&self) -> usize {
        self.sequence.entries
    }

    /// Whether the list is emtpy
    pub const fn is_empty(&self) -> bool {
        self.sequence.entries == 0
    }

    /// Find the first position for the given ID, counting from 1.
    pub fn find(&self, x: Id) -> usize {
        if x == 0 {
            return 0;
        }
        // hdt counts from 1
        // rsdict has nonzero value for 0, is that correct? adjust for that.
        self.bitmap.select1(x - 1).unwrap() as usize + 1
    }

    /// Return the position of element within the given bounds.
    /// # Arguments
    ///
    /// * `element` - a value that may or may not exist in the specified range of the list
    /// * `begin` - first index of the search range
    /// * `end` - end (exclusive) of the search range
    fn bin_search(&self, element: usize, begin: usize, end: usize) -> Option<usize> {
        let mut low = begin;
        let mut high = end;
        while low < high {
            let mid = usize::midpoint(low, high);
            match (self.sequence.get(mid) as usize).cmp(&element) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Find position of element y in the list x.
    // See <https://github.com/rdfhdt/hdt-cpp/blob/develop/libhdt/src/sequence/AdjacencyList.cpp>.
    pub fn search(&self, x: usize, y: usize) -> Option<usize> {
        self.bin_search(y, self.find(x), self.last(x) + 1)
    }

    /// Find the last position for the given ID, counting from 1.
    pub fn last(&self, x: Id) -> usize {
        self.find(x + 1) - 1
    }
}
