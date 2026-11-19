//! A split-block Bloom filter keyed by 64-bit hashes.
//!
//! The filter is an array of 256-bit blocks of eight `u32` words. A key's hash picks one block
//! with its upper 32 bits, and sets one bit in each of the eight words of that block from its
//! lower 32 bits multiplied by eight odd salts (the layout of Parquet's split-block filters). A
//! lookup reads a single cache line, and the filter has no false negatives. At 10 bits per key
//! about 1% of absent keys are reported as maybe present.

const SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947,
    0x5c6bfb31,
];
const MAGIC: &[u8; 4] = b"MSBF";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 4 + 1 + 4;

/// The splitmix64 hash of a dictionary code.
#[inline]
pub fn hash_code(code: u32) -> u64 {
    let mut z = (code as u64).wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitBlockBloom {
    blocks: Vec<[u32; 8]>,
}

impl SplitBlockBloom {
    /// An empty filter sized for `n_keys` keys at `bits_per_key` bits each (at least one block).
    pub fn new(n_keys: usize, bits_per_key: usize) -> Self {
        let bits = n_keys.saturating_mul(bits_per_key);
        let n_blocks = bits.div_ceil(256).max(1);
        Self {
            blocks: vec![[0u32; 8]; n_blocks],
        }
    }

    #[inline]
    fn block_index(&self, hash: u64) -> usize {
        (((hash >> 32) * self.blocks.len() as u64) >> 32) as usize
    }

    #[inline]
    fn masks(hash: u64) -> [u32; 8] {
        let x = hash as u32;
        let mut m = [0u32; 8];
        for i in 0..8 {
            m[i] = 1u32 << (x.wrapping_mul(SALT[i]) >> 27);
        }
        m
    }

    #[inline]
    pub fn insert(&mut self, hash: u64) {
        let i = self.block_index(hash);
        let m = Self::masks(hash);
        let b = &mut self.blocks[i];
        for w in 0..8 {
            b[w] |= m[w];
        }
    }

    /// False if the key was never inserted; true if it may have been.
    #[inline]
    pub fn may_contain(&self, hash: u64) -> bool {
        let b = &self.blocks[self.block_index(hash)];
        let m = Self::masks(hash);
        (0..8).all(|w| b[w] & m[w] != 0)
    }

    #[inline]
    pub fn insert_code(&mut self, code: u32) {
        self.insert(hash_code(code))
    }

    #[inline]
    pub fn may_contain_code(&self, code: u32) -> bool {
        self.may_contain(hash_code(code))
    }

    /// The size of the bit array in bytes.
    pub fn size_bytes(&self) -> usize {
        self.blocks.len() * 32
    }

    /// A self-describing little-endian encoding: magic, version, number of blocks, words.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.size_bytes());
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&(self.blocks.len() as u32).to_le_bytes());
        for b in &self.blocks {
            for w in b {
                out.extend_from_slice(&w.to_le_bytes());
            }
        }
        out
    }

    /// Reads [`Self::to_bytes`]; None if the bytes are not such an encoding.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC || bytes[4] != VERSION {
            return None;
        }
        let n_blocks = u32::from_le_bytes(bytes[5..9].try_into().ok()?) as usize;
        let body = &bytes[HEADER_LEN..];
        if n_blocks == 0 || body.len() != n_blocks * 32 {
            return None;
        }
        let blocks = body
            .chunks_exact(32)
            .map(|c| {
                let mut b = [0u32; 8];
                for (w, wb) in b.iter_mut().zip(c.chunks_exact(4)) {
                    *w = u32::from_le_bytes(wb.try_into().unwrap());
                }
                b
            })
            .collect();
        Some(Self { blocks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fpr(bits_per_key: usize) -> f64 {
        let n = 100_000u32;
        let mut f = SplitBlockBloom::new(n as usize, bits_per_key);
        // Keys spread out like dictionary codes of one table among many.
        for i in 0..n {
            f.insert_code(i * 3);
        }
        for i in 0..n {
            assert!(f.may_contain_code(i * 3), "false negative");
        }
        let probes = 200_000u32;
        let fp = (0..probes)
            .filter(|i| f.may_contain_code(i * 3 + 1))
            .count();
        fp as f64 / probes as f64
    }

    #[test]
    fn bounded_false_positives() {
        let r8 = fpr(8);
        let r10 = fpr(10);
        let r16 = fpr(16);
        assert!(r8 < 0.05, "8 bits per key: {r8}");
        assert!(r10 < 0.025, "10 bits per key: {r10}");
        assert!(r16 < 0.005, "16 bits per key: {r16}");
        assert!(r16 < r10 && r10 < r8);
    }

    #[test]
    fn tiny_filters_and_round_trip() {
        let mut f = SplitBlockBloom::new(0, 10);
        assert_eq!(f.size_bytes(), 32);
        assert!(!f.may_contain_code(7));
        f.insert_code(7);
        f.insert_code(u32::MAX);
        let g = SplitBlockBloom::from_bytes(&f.to_bytes()).unwrap();
        assert_eq!(f, g);
        assert!(g.may_contain_code(7) && g.may_contain_code(u32::MAX));
        assert!(SplitBlockBloom::from_bytes(b"MSBF").is_none());
        assert!(SplitBlockBloom::from_bytes(&f.to_bytes()[..20]).is_none());
        let mut bad = f.to_bytes();
        bad[0] = b'X';
        assert!(SplitBlockBloom::from_bytes(&bad).is_none());
    }
}
