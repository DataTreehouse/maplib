use super::*;
use std::collections::BTreeMap;

#[test]
fn split_compare_matches_concatenated_compare() {
    let strs = [
        "",
        "a",
        "ab",
        "abc",
        "abd",
        "b",
        "ba",
        "http://ex.org/a",
        "http://ex.org/ab",
        "http://ex.org#a",
        "http://ex.org/",
        "http://ex.org",
        "æøå",
        "æøb",
    ];
    for a in strs {
        for b in strs {
            for i in 0..=a.len() {
                if !a.is_char_boundary(i) {
                    continue;
                }
                for j in 0..=b.len() {
                    if !b.is_char_boundary(j) {
                        continue;
                    }
                    let got = cmp_split_strs((&a[..i], &a[i..]), (&b[..j], &b[j..]));
                    assert_eq!(got, a.cmp(b), "{a:?} split at {i} vs {b:?} split at {j}");
                }
            }
        }
    }
}

#[test]
fn distinct_codes_dense_and_sparse() {
    assert_eq!(distinct_codes(vec![5, 3, 5, 9, 3]), vec![3, 5, 9]);
    assert_eq!(
        distinct_codes(vec![4_000_000_000, 1, 1, 70]),
        vec![1, 70, 4_000_000_000]
    );
    assert!(distinct_codes(vec![]).is_empty());
}

#[test]
fn rank_map_dense_and_sparse() {
    let d = RankMap::from_pairs(vec![(10, 1), (12, 0)]);
    assert!(matches!(d, RankMap::Dense { .. }));
    assert_eq!(
        (d.get(10), d.get(12), d.get(11), d.get(9)),
        (Some(1), Some(0), None, None)
    );
    assert_eq!(d.get(1000), None);
    let s = RankMap::from_pairs(vec![(4_000_000_000, 0), (2, 1)]);
    assert!(matches!(s, RankMap::Sorted { .. }));
    assert_eq!(
        (s.get(2), s.get(4_000_000_000), s.get(3)),
        (Some(1), Some(0), None)
    );
}

/// Small deterministic random number generator for the tests.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn random_string(rng: &mut Rng, is_iri: bool) -> String {
    let prefixes = [
        "http://ex.org/",
        "http://ex.org/ns#",
        "urn:x:",
        "http://other.org/a/b/",
        "",
    ];
    let alphabet = ['a', 'b', 'c', 'æ', '/', '#', ':', 'z', '0'];
    let mut s = String::new();
    if is_iri {
        s.push_str(prefixes[rng.below(prefixes.len() as u64) as usize]);
    }
    for _ in 0..rng.below(6) {
        s.push(alphabet[rng.below(alphabet.len() as u64) as usize]);
    }
    s
}

/// Checks a map against a reference of string -> code.
fn check(m: &CatMapsInMemory, reference: &BTreeMap<String, u32>) {
    assert_eq!(m.height() as usize, reference.len());
    assert_eq!(m.is_empty(), reference.is_empty());
    for (s, u) in reference {
        assert_eq!(m.maybe_encode_in_memory_str(s), Some(*u), "encode {s:?}");
        assert_eq!(m.maybe_decode(u).as_deref(), Some(s.as_str()), "decode {u}");
        assert!(m.contains_u32(u));
    }
    // Order: global ranks and the record batch follow string order.
    let g = m.global_ranks();
    let mut by_rank: Vec<_> = reference
        .values()
        .map(|u| (g.get(*u).unwrap(), *u))
        .collect();
    by_rank.sort();
    let in_rank_order: Vec<u32> = by_rank.iter().map(|x| x.1).collect();
    let in_string_order: Vec<u32> = reference.values().copied().collect();
    assert_eq!(in_rank_order, in_string_order);
    let rb = m.to_record_batch();
    let strs = rb
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let got: Vec<String> = strs.iter().map(|x| x.unwrap().to_string()).collect();
    let want: Vec<String> = reference.keys().cloned().collect();
    assert_eq!(got, want);
    if !reference.is_empty() {
        assert_eq!(m.counter(), reference.values().max().unwrap() + 1);
        let codes: Vec<u32> = distinct_codes(reference.values().copied().collect());
        let r = m.sort_ranks(&codes);
        let mut by_sort_rank: Vec<_> = codes.iter().map(|u| (r.get(*u).unwrap(), *u)).collect();
        by_sort_rank.sort();
        assert_eq!(
            by_sort_rank.iter().map(|x| x.1).collect::<Vec<_>>(),
            in_string_order
        );
    }
}

#[test]
fn randomized_against_reference() {
    for is_iri in [true, false] {
        let mut rng = Rng(0x1234_5678 + is_iri as u64);
        let mut m = CatMapsInMemory::new_empty(is_iri);
        let mut reference: BTreeMap<String, u32> = BTreeMap::new();
        let mut c = 0u32;
        for round in 0..400 {
            match rng.below(6) {
                // Single inserts
                0 | 1 => {
                    for _ in 0..rng.below(40) {
                        let s = random_string(&mut rng, is_iri);
                        if m.maybe_encode_in_memory_str(&s).is_none() {
                            m.encode_new_in_memory_string(Cow::Owned(s.clone()), c);
                            reference.insert(s, c);
                            c += 1;
                        }
                    }
                }
                // Merge of another map with its own codes
                2 => {
                    let mut other = CatMapsInMemory::new_empty(is_iri);
                    let mut other_ref = BTreeMap::new();
                    let base = 1_000_000 + round * 1000;
                    for i in 0..rng.below(50) {
                        let s = random_string(&mut rng, is_iri);
                        if other.maybe_encode_in_memory_str(&s).is_none() {
                            other.encode_new_in_memory_string(
                                Cow::Owned(s.clone()),
                                base + i as u32,
                            );
                            other_ref.insert(s, base + i as u32);
                        }
                    }
                    let remap = m.merge(&other, &mut c);
                    for (s, ou) in &other_ref {
                        let u = *reference.entry(s.clone()).or_insert_with(|| remap[ou]);
                        assert_eq!(remap[ou], u);
                    }
                    // The join re-encoding finds the shared strings with differing codes.
                    let mut join = other.inner_join_re_enc(&m);
                    join.sort();
                    let mut want: Vec<_> = other_ref
                        .iter()
                        .map(|(s, ou)| (*ou, reference[s]))
                        .filter(|(a, b)| a != b)
                        .collect();
                    want.sort();
                    assert_eq!(join, want);
                }
                // Garbage collection of some codes
                3 => {
                    let victims: Vec<u32> = reference
                        .values()
                        .copied()
                        .filter(|_| rng.below(4) == 0)
                        .collect();
                    m.garbage_collect_cats(RangeSetBlaze::from_iter(victims.iter().copied()));
                    reference.retain(|_, u| !victims.contains(u));
                    for u in &victims {
                        assert!(!m.contains_u32(u));
                    }
                }
                // Compaction renumbers in string order
                4 => {
                    m.compact();
                    for (i, u) in reference.values_mut().enumerate() {
                        *u = i as u32;
                    }
                    c = reference.len() as u32;
                }
                // Image of some codes
                _ => {
                    let keep: HashSet<u32> = reference
                        .values()
                        .copied()
                        .filter(|_| rng.below(3) == 0)
                        .chain([u32::MAX - 5])
                        .collect();
                    let img = m.image(&keep);
                    let img_ref: BTreeMap<String, u32> = reference
                        .iter()
                        .filter(|(_, u)| keep.contains(u))
                        .map(|(s, u)| (s.clone(), *u))
                        .collect();
                    if img_ref.is_empty() {
                        assert!(img.is_none());
                    } else {
                        check(&img.unwrap(), &img_ref);
                    }
                }
            }
            check(&m, &reference);
            assert_eq!(
                m.range_set(),
                RangeSetBlaze::from_iter(reference.values().copied())
            );
        }
        // Spaced strings cover the map in order.
        if !reference.is_empty() {
            let spaced = m.get_spaced_strings(7);
            assert_eq!(spaced.first().unwrap().0, *reference.keys().next().unwrap());
            assert_eq!(spaced.last().unwrap().1, *reference.keys().last().unwrap());
        }
    }
}

#[test]
fn local_maps_with_high_and_spread_codes() {
    let mut m = CatMapsInMemory::new_singular("http://ex.org/a", 20_000_000, true);
    m.encode_new_in_memory_string(Cow::Borrowed("http://ex.org/b"), 20_000_001);
    m.encode_new_in_memory_string(Cow::Borrowed("http://ex.org/c"), 3);
    m.encode_new_in_memory_string(Cow::Borrowed("x"), 4_000_000_000);
    for (s, u) in [
        ("http://ex.org/a", 20_000_000),
        ("http://ex.org/b", 20_000_001),
        ("http://ex.org/c", 3),
        ("x", 4_000_000_000),
    ] {
        assert_eq!(m.maybe_encode_in_memory_str(s), Some(u));
        assert_eq!(m.maybe_decode(&u).unwrap(), s);
    }
    assert!(matches!(m.codes, CodeIndex::Sparse(_)));
    let batch = m.decode_batch(&[Some(3), None, Some(7), Some(20_000_001)]);
    assert_eq!(batch[0].as_deref(), Some("http://ex.org/c"));
    assert_eq!(batch[1], None);
    assert_eq!(batch[2], None);
    assert_eq!(batch[3].as_deref(), Some("http://ex.org/b"));
}

#[test]
fn long_strings_and_from_ordered_vec() {
    let long = "y".repeat(MAX_CHUNK + 10);
    let ss = vec![
        (Cow::Borrowed("b"), 1u32),
        (Cow::Owned(long.clone()), 0),
        (Cow::Borrowed(""), 2),
    ];
    let m = CatMapsInMemory::from_ordered_vec(ss, false);
    assert_eq!(m.maybe_encode_in_memory_str(&long), Some(0));
    assert_eq!(m.maybe_encode_in_memory_str(""), Some(2));
    assert_eq!(m.maybe_decode(&0).unwrap().len(), long.len());
    let g = m.global_ranks();
    assert_eq!((g.get(2), g.get(1), g.get(0)), (Some(0), Some(1), Some(2)));
}

#[test]
fn distinct_strs_and_insert_new_strs() {
    let words: Vec<String> = (0..5000)
        .map(|i| format!("http://ex.org/w{}", i % 1700))
        .collect();
    let refs: Vec<&str> = words.iter().map(|s| s.as_str()).collect();
    let d = distinct_strs(&refs);
    assert_eq!(d.len(), 1700);
    let set: HashSet<&str> = d.iter().copied().collect();
    assert_eq!(set.len(), 1700);
    let mut m = CatMapsInMemory::new_singular("http://ex.org/w5", 0, true);
    let new: Vec<&str> = d
        .iter()
        .copied()
        .filter(|s| *s != "http://ex.org/w5")
        .collect();
    m.insert_new_strs(&new, 1);
    assert_eq!(m.height() as usize, 1700);
    for (i, s) in new.iter().enumerate() {
        assert_eq!(m.maybe_encode_in_memory_str(s), Some(1 + i as u32));
        let (p, q) = m.decode_parts(1 + i as u32).unwrap();
        assert_eq!(format!("{p}{q}"), *s);
    }
    assert_eq!(m.decode_parts(99_999), None);
}

#[test]
fn has_string_with_prefix_compressed_and_not() {
    for is_iri in [true, false] {
        let mut m = CatMapsInMemory::new_empty(is_iri);
        let strs = [
            "http://ex.org/a/b",
            "http://ex.org/a/c",
            "http://other.org/x",
            "urn:x",
            "abc",
        ];
        for (i, s) in strs.iter().enumerate() {
            m.encode_new_in_memory_string(std::borrow::Cow::Borrowed(s), i as u32);
        }
        for p in ["", "h", "http://ex.org/", "http://ex.org/a/b", "http://other", "urn:", "ab", "abc"] {
            assert!(m.has_string_with_prefix(p), "{p} {is_iri}");
        }
        for p in ["http://ex.org/a/bb", "http://ex.org/b", "x", "abcd", "http://ex.org/a/b/"] {
            assert!(!m.has_string_with_prefix(p), "{p} {is_iri}");
        }
    }
    assert!(!CatMapsInMemory::new_empty(true).has_string_with_prefix(""));
}
