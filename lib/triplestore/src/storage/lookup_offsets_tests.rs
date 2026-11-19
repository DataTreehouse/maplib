use super::{
    get_lookup_offsets, lookup_offsets_seek, lookup_offsets_should_seek, lookup_offsets_walk,
};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A small xorshift generator, so that the test needs no dependency and is reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// A random string from a small alphabet, so that targets often equal values and keys.
fn random_string(rng: &mut Rng) -> String {
    let len = 1 + rng.below(3);
    (0..len)
        .map(|_| (b'a' + rng.below(4) as u8) as char)
        .collect()
}

/// A sorted column and its sparse index, built as `create_sparse_string_index` builds it, with
/// a key at every `step` rows and at the last row, keeping the first row of a repeated key.
fn random_column(
    rng: &mut Rng,
    height: usize,
    step: usize,
) -> (Vec<String>, BTreeMap<Arc<String>, usize>) {
    let mut column: Vec<String> = (0..height).map(|_| random_string(rng)).collect();
    column.sort();
    let mut sparse_map = BTreeMap::new();
    let mut offset = 0;
    while offset < height {
        sparse_map
            .entry(Arc::new(column[offset].clone()))
            .or_insert(offset);
        offset += step;
    }
    sparse_map
        .entry(Arc::new(column[height - 1].clone()))
        .or_insert(height - 1);
    (column, sparse_map)
}

/// Sorted targets: values in the column, keys of the index, values before the first key and
/// after the last key, and values that are not in the column.
fn random_targets(
    rng: &mut Rng,
    column: &[String],
    sparse_map: &BTreeMap<Arc<String>, usize>,
) -> Vec<String> {
    let keys: Vec<&Arc<String>> = sparse_map.keys().collect();
    let n = rng.below(12);
    let mut trgs: Vec<String> = (0..n)
        .map(|_| match rng.below(5) {
            0 => column[rng.below(column.len())].clone(),
            1 => keys[rng.below(keys.len())].as_str().to_string(),
            // Before every key, as the alphabet starts at 'a'.
            2 => "A".to_string(),
            // After every key.
            3 => "z".to_string(),
            _ => random_string(rng),
        })
        .collect();
    trgs.sort();
    if rng.below(4) == 0 {
        trgs.dedup();
    }
    trgs
}

#[test]
fn seek_and_walk_give_identical_ranges() {
    let mut rng = Rng(0x9e3779b97f4a7c15);
    for _ in 0..20_000 {
        let height = 1 + rng.below(300);
        let step = 1 + rng.below(12);
        let (column, sparse_map) = random_column(&mut rng, height, step);
        let trgs = random_targets(&mut rng, &column, &sparse_map);
        let trgs: Vec<&str> = trgs.iter().map(|x| x.as_str()).collect();

        let walk = lookup_offsets_walk(&trgs, &sparse_map, height);
        let seek = lookup_offsets_seek(&trgs, &sparse_map, height);
        assert_eq!(
            walk, seek,
            "targets {trgs:?}, index {sparse_map:?}, height {height}"
        );
        assert_eq!(get_lookup_offsets(&trgs, &sparse_map, height), walk);

        // Every row holding a target lies in one of the ranges.
        for (row, value) in column.iter().enumerate() {
            if trgs.contains(&value.as_str()) {
                assert!(
                    walk.iter()
                        .any(|(offset, len)| *offset <= row && row < offset + len),
                    "row {row} ({value}) not in {walk:?}"
                );
            }
        }
    }
}

#[test]
fn seek_is_chosen_for_few_targets() {
    assert!(lookup_offsets_should_seek(0, 0));
    assert!(lookup_offsets_should_seek(1, 1));
    assert!(lookup_offsets_should_seek(1, 100_000));
    assert!(lookup_offsets_should_seek(5_000, 100_000));
    assert!(!lookup_offsets_should_seek(10_000, 100_000));
    assert!(!lookup_offsets_should_seek(3, 4));
    assert!(!lookup_offsets_should_seek(usize::MAX, 100_000));
}
