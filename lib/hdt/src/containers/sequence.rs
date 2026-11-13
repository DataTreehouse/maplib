use super::vbyte::encode_vbyte;
use crate::containers::vbyte::read_vbyte;
use bytesize::ByteSize;
use std::fmt;
use std::io::{BufRead, Write};
use std::mem::size_of;

const USIZE_BITS: usize = usize::BITS as usize;

pub type Result<T> = core::result::Result<T, Error>;

/// Integer sequence with a given number of bits, which means numbers may be represented along byte boundaries.
/// Also called "array" in the HDT spec, only Log64 is supported.
/// However no documentation for Log64 could be found, it seems to be just "normal" bit packing.
/// In the HDT serialization, it is byte-aligned but for performance reasons we use a bigger block size here like usize or u64 and fill the rest with zeroes.
/// It is not tested whether u64 or usize gives the best performance on a 32 Bit target like WASM32 which is run on a 64 Bit CPU.
/// This type is not optimized for used for general integer sequences because other libraries already implement that e.g. sucds CompactVector.
/// Instead it is meant to be read from and written to an HDT file, as such it doesn't have good interoperability.
/// We could still use an off-the-shelf library for the data which may be even more optimized but they often don't allow you to write or read to the internal data which makes constructing and writing it less comfortable and performant.
// Update: Now that we are evaluating switching from sucds to QWT, which does not seem to contain such a type, such helper functions could be useful.
//#[derive(Clone)]
pub struct Sequence {
    /// Number of integers in the sequence.
    pub entries: usize,
    /// Number of bits that each integer uses.
    pub bits_per_entry: usize,
    /// Data in blocks.
    pub data: Vec<usize>,
}

enum SequenceType {
    Log64 = 1,
    #[allow(dead_code)]
    UInt32 = 2,
    #[allow(dead_code)]
    UInt64 = 3,
}

impl TryFrom<u8> for SequenceType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(SequenceType::Log64),
            _ => Err(Error::UnsupportedSequenceType(value)),
        }
    }
}

/// The error type for the sequence read function.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Invalid CRC8-CCIT checksum {0}, expected {1}")]
    InvalidCrc8Checksum(u8, u8),
    #[error("Invalid CRC32C checksum {0}, expected {1}")]
    InvalidCrc32Checksum(u32, u32),
    #[error("Failed to turn raw bytes into usize")]
    TryFromSliceError(#[from] std::array::TryFromSliceError),
    #[error("invalid LogArray type {0} != 1")]
    UnsupportedSequenceType(u8),
    #[error("entry size of {0} bit too large (>64 bit)")]
    EntrySizeTooLarge(usize),
}

impl fmt::Debug for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} with {} entries, {} bits per entry, starting with {:?}",
            ByteSize(self.size_in_bytes() as u64),
            self.entries,
            self.bits_per_entry,
            self.into_iter().take(10).collect::<Vec::<_>>()
        )
    }
}

pub struct SequenceIter<'a> {
    sequence: &'a Sequence,
    i: usize,
}

impl Iterator for SequenceIter<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.sequence.entries {
            return None;
        }
        let e = self.sequence.get(self.i);
        self.i += 1;
        Some(e)
    }
}

impl<'a> IntoIterator for &'a Sequence {
    type Item = usize;
    type IntoIter = SequenceIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        SequenceIter { sequence: self, i: 0 }
    }
}

impl Sequence {
    /// Get the integer at the given index, counting from 0.
    /// Panics if the index is out of bounds.
    pub fn get(&self, index: usize) -> usize {
        let scaled_index = index * self.bits_per_entry;
        let block_index = scaled_index / USIZE_BITS;
        let bit_index = scaled_index % USIZE_BITS;

        let mut result;

        let result_shift = USIZE_BITS - self.bits_per_entry;
        if bit_index + self.bits_per_entry <= USIZE_BITS {
            let block_shift = USIZE_BITS - bit_index - self.bits_per_entry;
            result = (self.data[block_index] << block_shift) >> result_shift;
        } else {
            let block_shift = (USIZE_BITS << 1) - bit_index - self.bits_per_entry;
            result = self.data[block_index] >> bit_index;
            result |= (self.data[block_index + 1] << block_shift) >> result_shift;
        }
        result
    }

    /// Size in bytes on the heap.
    pub const fn size_in_bytes(&self) -> usize {
        (self.data.len() * USIZE_BITS) >> 3
    }

    /// Read sequence including metadata from HDT data.
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self> {
        // read entry metadata
        // keep track of history for CRC8
        let mut history = Vec::<u8>::new();

        // read and validate type
        let mut buffer = [0_u8];
        reader.read_exact(&mut buffer)?;
        history.extend_from_slice(&buffer);
        SequenceType::try_from(buffer[0])?;

        // read number of bits per entry
        let mut buffer = [0_u8];
        reader.read_exact(&mut buffer)?;
        history.extend_from_slice(&buffer);
        let bits_per_entry = buffer[0] as usize;
        if bits_per_entry > USIZE_BITS {
            return Err(Error::EntrySizeTooLarge(bits_per_entry));
        }

        // read number of entries
        let (entries, bytes_read) = read_vbyte(reader)?;
        history.extend_from_slice(&bytes_read);

        // read entry metadata CRC8
        let mut crc_code = [0_u8];
        reader.read_exact(&mut crc_code)?;
        let crc_code = crc_code[0];

        // validate entry metadata CRC8
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest = crc8.digest();
        digest.update(&history);

        let crc_calculated = digest.finalize();
        if crc_calculated != crc_code {
            return Err(Error::InvalidCrc8Checksum(crc_calculated, crc_code));
        }

        // read body data
        // read all but the last entry, since the last one is byte aligned
        let total_bits = bits_per_entry * entries;
        let full_byte_amount = (total_bits.div_ceil(USIZE_BITS).saturating_sub(1)) * size_of::<usize>();
        let mut full_words = vec![0_u8; full_byte_amount];
        reader.read_exact(&mut full_words)?;
        let mut data: Vec<usize> = Vec::with_capacity(full_byte_amount / size_of::<usize>() + 2);
        // read entry body

        // turn the raw bytes into usize values
        for word in full_words.chunks_exact(size_of::<usize>()) {
            data.push(usize::from_le_bytes(<[u8; size_of::<usize>()]>::try_from(word)?));
        }

        // keep track of history for CRC32
        let mut history = full_words;
        // read the last few bits, byte aligned
        let mut bits_read = 0;
        let mut last_value: usize = 0;
        let last_entry_bits = if total_bits == 0 { 0 } else { ((total_bits - 1) % USIZE_BITS) + 1 };

        while bits_read < last_entry_bits {
            let mut buffer = [0u8];
            reader.read_exact(&mut buffer)?;
            history.extend_from_slice(&buffer);
            last_value |= (buffer[0] as usize) << bits_read;
            bits_read += 8;
        }
        data.push(last_value);
        // read entry body CRC32
        let mut crc_code = [0_u8; 4];
        reader.read_exact(&mut crc_code)?;

        let crc_code32 = u32::from_le_bytes(crc_code);
        //let start = std::time::Instant::now();
        // validate entry body CRC32
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest = crc32.digest();
        digest.update(&history);
        let crc_calculated32 = digest.finalize();
        //println!("Sequence of {} validated in {:?}", ByteSize(history.len() as u64), start.elapsed());
        if crc_calculated32 != crc_code32 {
            return Err(Error::InvalidCrc32Checksum(crc_calculated32, crc_code32));
        }

        Ok(Sequence { entries, bits_per_entry, data })
    }

    /// save sequence per HDT spec using CRC
    pub fn write(&self, dest_writer: &mut impl Write) -> Result<()> {
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest = crc8.digest();
        // libhdt/src/sequence/LogSequence2.cpp::save()
        // Write offsets using variable-length encoding
        let seq_type: [u8; 1] = [1];
        dest_writer.write_all(&seq_type)?;
        digest.update(&seq_type);
        // Write numbits
        let bits_per_entry: [u8; 1] = [self.bits_per_entry.try_into().unwrap()];
        dest_writer.write_all(&bits_per_entry)?;
        digest.update(&bits_per_entry);
        // Write numentries
        let buf = &encode_vbyte(self.entries);
        dest_writer.write_all(buf)?;
        digest.update(buf);
        let checksum: u8 = digest.finalize();
        dest_writer.write_all(&[checksum])?;

        // Write data
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest32 = crc32.digest();
        let bytes: Vec<u8> = self.data.iter().flat_map(|&val| val.to_le_bytes()).collect();
        //  unused zero bytes in the last usize are not written
        let num_bytes = (self.bits_per_entry * self.entries).div_ceil(8);
        let bytes = &bytes[..num_bytes];
        dest_writer.write_all(bytes)?;
        digest32.update(bytes);
        let checksum32 = digest32.finalize();
        dest_writer.write_all(&checksum32.to_le_bytes())?;
        dest_writer.flush()?;
        Ok(())
    }

    /*
        // this is the new one using sucds but we are back to the old manual code + pack_bits with qwt for now
        /// Pack the given integers., which have to fit into the given number of bits.
        // pub fn new(nums: &[usize], bits_per_entry: usize) -> Sequence {
        pub fn new(nums: &[usize]) -> Sequence {
            let entries = nums.len();
            if entries == 0 {
                return Sequence { entries, bits_per_entry: 0, data: vec![] };
            }
            let bits_per_entry = nums.iter().max().unwrap().bit_width() as usize; // nightly only
            let data = Vec::<usize>::new();
            panic!("manual bit packing not implemented yet");
            //let mut cv = CompactVector::with_capacity(nums.len(), bits_per_entry).expect("value too large");
            // let cv = CompactVector::from_slice(nums).unwrap();
            // let bits_per_entry = cv.width();
            // let data = cv.into_bit_vector().into_words();
            Sequence { entries, bits_per_entry, data }
        }
    }
    */

    // could also determine bits per entry using max of numbers but that would take time
    // pub fn new(numbers: &[usize], bits_per_entry: usize) -> Sequence {
    pub fn new(numbers: &[usize]) -> Sequence {
        let entries = numbers.len();
        if entries == 0 {
            return Sequence { entries, bits_per_entry: 0, data: vec![] };
        }
        //let bits_per_entry = numbers.iter().max().unwrap().bit_width() as usize; // nightly only
        let bits_per_entry = (usize::BITS - numbers.iter().max().unwrap().leading_zeros()) as usize; // emulate bit_width using stable API
        let numbers8 = Self::pack_bits(numbers, bits_per_entry);
        // reuse pack_bits by Greg Hanson, which is designed for writing directly, and put it
        // into usize chunks, could also rewrite pack_bits for usize later but first get a functioning prototype
        let bytes = numbers8.len();
        let rest_byte_amount = bytes % size_of::<usize>();
        let full_byte_amount = bytes - rest_byte_amount;
        let mut data = Vec::<usize>::new();
        let full_words = &numbers8[..full_byte_amount];
        for word in full_words.chunks_exact(size_of::<usize>()) {
            data.push(usize::from_le_bytes(<[u8; size_of::<usize>()]>::try_from(word).unwrap()));
        }
        if rest_byte_amount > 0 {
            let mut last = [0u8; size_of::<usize>()];
            last[..rest_byte_amount].copy_from_slice(&numbers8[full_byte_amount..]);
            data.push(usize::from_le_bytes(last));
        }
        Sequence { entries, bits_per_entry, data }
    }

    // manual compact integer sequence, as sucds lib does not allow export of internal storage
    fn pack_bits(numbers: &[usize], bits_per_entry: usize) -> Vec<u8> {
        let mut output = Vec::new();
        let mut current_byte = 0u8;
        let mut bit_offset = 0;

        for value in numbers {
            let mut val = value & ((1 << bits_per_entry) - 1); // mask to get only relevant bits
            let mut bits_left = bits_per_entry;

            while bits_left > 0 {
                let available = 8 - bit_offset;
                let to_write = bits_left.min(available);

                // Shift bits to align with current byte offset
                current_byte |= ((val & ((1 << to_write) - 1)) as u8) << bit_offset;

                bit_offset += to_write;
                val >>= to_write;
                bits_left -= to_write;

                if bit_offset == 8 {
                    output.push(current_byte);
                    current_byte = 0;
                    bit_offset = 0;
                }
            }
        }

        // Push final byte if there's remaining bits
        if bit_offset > 0 {
            output.push(current_byte);
        }
        output
    }

    /// Same as [`Self::new`] but accepts `&[u32]` values directly, avoiding a
    /// `Vec<usize>` intermediate. The values are widened to `usize` during
    /// bit-packing only — no separate `Vec<usize>` is ever allocated.
    ///
    /// Used by the op-index builder where every value fits in `u32` (they
    /// are Y-level positions capped at ~2^28 for any realistic HDT).
    pub fn new_from_u32(numbers: &[u32]) -> Sequence {
        let entries = numbers.len();
        if entries == 0 {
            return Sequence { entries, bits_per_entry: 0, data: vec![] };
        }
        let bits_per_entry = (u32::BITS - numbers.iter().max().unwrap().leading_zeros()) as usize;

        // Bit-pack directly from u32, same algorithm as pack_bits but
        // widening u32→usize on the fly instead of requiring &[usize].
        let mut output = Vec::<u8>::with_capacity((entries * bits_per_entry).div_ceil(8));
        let mut current_byte = 0u8;
        let mut bit_offset = 0usize;
        for &value in numbers {
            let mut val = value as usize & ((1usize << bits_per_entry) - 1);
            let mut bits_left = bits_per_entry;
            while bits_left > 0 {
                let available = 8 - bit_offset;
                let to_write = bits_left.min(available);
                current_byte |= ((val & ((1 << to_write) - 1)) as u8) << bit_offset;
                bit_offset += to_write;
                val >>= to_write;
                bits_left -= to_write;
                if bit_offset == 8 {
                    output.push(current_byte);
                    current_byte = 0;
                    bit_offset = 0;
                }
            }
        }
        if bit_offset > 0 {
            output.push(current_byte);
        }

        // Repack u8 → usize words (same as Sequence::new).
        let bytes = output.len();
        let rest_byte_amount = bytes % size_of::<usize>();
        let full_byte_amount = bytes - rest_byte_amount;
        let mut data = Vec::<usize>::new();
        for word in output[..full_byte_amount].chunks_exact(size_of::<usize>()) {
            data.push(usize::from_le_bytes(<[u8; size_of::<usize>()]>::try_from(word).unwrap()));
        }
        if rest_byte_amount > 0 {
            let mut last = [0u8; size_of::<usize>()];
            last[..rest_byte_amount].copy_from_slice(&output[full_byte_amount..]);
            data.push(usize::from_le_bytes(last));
        }
        Sequence { entries, bits_per_entry, data }
    }
}
