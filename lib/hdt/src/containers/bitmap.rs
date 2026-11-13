//! Bitmap with rank and select support read from an HDT file.
use crate::containers::vbyte::{encode_vbyte, read_vbyte};
use bytesize::ByteSize;
use qwt::mem_dbg::{MemSize, SizeFlags};
use qwt::{AccessBin, BitVector, BitVectorMut, RankBin, SelectBin, bitvector::rs_narrow::RSNarrow};
use std::fmt;
use std::io::BufRead;
use std::mem::size_of;

/// Compact bitmap representation with rank and select support.
#[derive(Clone)]
pub struct Bitmap {
    /// should be private but is needed by containers/bitmap.rs, use methods provided by Bitmap
    pub dict: RSNarrow,
}

pub type Result<T> = core::result::Result<T, Error>;

/// The error type for the bitmap read function.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Invalid CRC8-CCIT checksum {0}, expected {1}")]
    InvalidCrc8Checksum(u8, u8),
    #[error("Invalid CRC32C checksum {0}, expected {1}")]
    InvalidCrc32Checksum(u32, u32),
    #[error("Failed to turn raw bytes into u64")]
    TryFromSliceError(#[from] std::array::TryFromSliceError),
    #[error("Read unsupported bitmap type {0} != 1")]
    UnsupportedBitmapType(u8),
}

impl fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {} bits", ByteSize(self.size_in_bytes() as u64), self.len())
    }
}

impl From<BitVector> for Bitmap {
    fn from(bv: BitVector) -> Self {
        Bitmap { dict: bv.into() }
    }
}

impl From<BitVectorMut> for Bitmap {
    fn from(bv: BitVectorMut) -> Self {
        Bitmap { dict: <BitVectorMut as Into<BitVector>>::into(bv).into() }
    }
}

impl Bitmap {
    /// Construct a bitmap from an existing bitmap in form of a vector, which doesn't have rank and select support. Number of bits multiple of 64.
    pub fn new(data: &[u64]) -> Self {
        let v: BitVector = BitVectorMut::from_packed_data(data, data.len() * 64).into();
        Bitmap { dict: v.into() }
    }

    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.dict.mem_size(SizeFlags::default())
    }

    /// Number of bits in the bitmap, multiple of 64
    pub fn len(&self) -> usize {
        self.dict.count_zeros() + self.dict.count_ones() // RSNarrow.len() is not public
        // self.dict.bv_len() // only on RSWide
    }

    /// Number of bits set
    pub fn num_ones(&self) -> usize {
        self.dict.count_ones()
    }

    /// Returns the position of the k-1-th one bit or None if there aren't that many.
    pub fn select1(&self, k: usize) -> Option<usize> {
        self.dict.select1(k)
    }

    /// Returns the number of one bits from the 0-th bit to the k-1-th bit. Panics if self.len() < pos.
    pub fn rank(&self, k: usize) -> usize {
        self.dict.rank1(k).unwrap_or_else(|| panic!("Out of bounds position: {} >= {}", k, self.len()))
    }

    /// Whether the node given position is the last child of its parent.
    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        self.dict.get(word_index).expect("word index out of bounds")
    }

    /// Read bitmap from a suitable point within HDT file data and verify checksums.
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self> {
        use Error::*;
        let mut history: Vec<u8> = Vec::with_capacity(5);

        // read the type
        let mut bitmap_type = [0u8];
        reader.read_exact(&mut bitmap_type)?;
        history.extend_from_slice(&bitmap_type);
        if bitmap_type[0] != 1 {
            return Err(UnsupportedBitmapType(bitmap_type[0]));
        }

        // read the number of bits
        let (num_bits, bytes_read) = read_vbyte(reader)?;
        history.extend_from_slice(&bytes_read);

        // read section CRC8
        let mut crc_code = [0_u8];
        reader.read_exact(&mut crc_code)?;
        let crc_code = crc_code[0];

        // validate section CRC8
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest = crc8.digest();
        digest.update(&history);
        let crc_calculated = digest.finalize();
        if crc_calculated != crc_code {
            return Err(InvalidCrc8Checksum(crc_calculated, crc_code));
        }

        // read all but the last word, last word is byte aligned
        let full_byte_amount = (num_bits.saturating_sub(1) >> 6) * 8;
        let mut full_words = vec![0_u8; full_byte_amount];
        // div_ceil is unstable
        let mut data: Vec<u64> = Vec::with_capacity(full_byte_amount / 8 + usize::from(full_byte_amount % 8 != 0));
        reader.read_exact(&mut full_words)?;

        for word in full_words.chunks_exact(size_of::<u64>()) {
            data.push(u64::from_le_bytes(<[u8; 8]>::try_from(word)?));
        }

        // initiate computation of CRC32
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest = crc32.digest();
        digest.update(&full_words);

        let mut bits_read = 0;
        let mut last_value: u64 = 0;
        let last_word_bits = if num_bits == 0 { 0 } else { ((num_bits - 1) % 64) + 1 };

        while bits_read < last_word_bits {
            let mut buffer = [0u8];
            reader.read_exact(&mut buffer)?;
            digest.update(&buffer);
            last_value |= (buffer[0] as u64) << bits_read;
            bits_read += 8;
        }
        data.push(last_value);

        // read entry body CRC32
        let mut crc_code = [0_u8; 4];
        reader.read_exact(&mut crc_code)?;
        let crc_code = u32::from_le_bytes(crc_code);

        // validate entry body CRC32
        // not worth it to spawn an extra thread as our bitmaps are comparatively small
        let crc_calculated = digest.finalize();
        if crc_calculated != crc_code {
            return Err(InvalidCrc32Checksum(crc_calculated, crc_code));
        }
        Ok(Self::new(&data))
    }

    pub fn write(&self, w: &mut impl std::io::Write) -> Result<()> {
        let crc = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut hasher = crc.digest();
        // type
        let bitmap_type: [u8; 1] = [1];
        w.write_all(&bitmap_type)?;
        hasher.update(&bitmap_type);
        // number of bits
        let t = encode_vbyte(self.len());
        w.write_all(&t)?;
        hasher.update(&t);
        // crc8 checksum
        let checksum = hasher.finalize();
        w.write_all(&checksum.to_le_bytes())?;

        // write data
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut hasher = crc32.digest();

        let words: &[u64] = self.dict.bit_vector().words();
        let num_bytes = self.dict.bit_vector().len().div_ceil(8); // HDT spec expects no superflous bytes to be written
        let bytes = unsafe {
            std::slice::from_raw_parts(words.as_ptr().cast::<u8>(), num_bytes) // assume little endian
        };

        w.write_all(bytes)?;
        hasher.update(bytes);
        let crc_code = hasher.finalize();
        let crc_code = crc_code.to_le_bytes();
        w.write_all(&crc_code)?;
        Ok(())
    }
}
