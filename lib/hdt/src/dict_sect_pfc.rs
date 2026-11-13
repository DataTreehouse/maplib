#![allow(missing_docs)] // temporariy while we figure out what should be public in the end
/// Dictionary section with plain front coding.
/// See <https://www.rdfhdt.org/hdt-binary-format/#DictionarySectionPlainFrontCoding>.
use crate::containers::vbyte::{decode_vbyte_delta, encode_vbyte, read_vbyte};
use crate::containers::{Sequence, sequence};
use crate::triples::Id;
use bytesize::ByteSize;
use std::cmp::{Ordering, min};
use std::collections::BTreeSet;
use std::io::{BufRead, Write};
use std::sync::Arc;
#[cfg(not(any(target_arch = "wasm32", target_arch = "wasm64")))]
use std::thread::{JoinHandle, spawn};
use std::{fmt, str};
use thiserror::Error;

pub type Result<T> = core::result::Result<T, Error>;

/// Type alias for the internal read result to reduce complexity
type ReadInternalResult = (usize, usize, Sequence, Arc<[u8]>, [u8; 4]);

/// Dictionary section with plain front coding.
//#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DictSectPFC {
    /// total number of strings stored
    pub num_strings: usize,
    /// the last block may have less than "block_size" strings
    pub block_size: usize,
    /// stores the starting position of each block
    pub sequence: Sequence,
    /// the substrings
    pub packed_data: Arc<[u8]>,
}

/// The error type for the DictSectPFC read function.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Invalid CRC8-CCIT checksum {0}, expected {1}")]
    InvalidCrc8Checksum(u8, u8),
    #[error("Invalid CRC32-C checksum {0}, expected {1}")]
    InvalidCrc32Checksum(u32, u32),
    #[error("implementation only supports plain front coded dictionary section type 2, found type {0}")]
    DictSectNotPfc(u8),
    #[error("sequence read error")]
    Sequence(#[from] sequence::Error),
}

impl fmt::Debug for DictSectPFC {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "total size {}, {} strings, sequence {:?}, packed data {:?}",
            ByteSize(self.size_in_bytes() as u64),
            self.num_strings,
            self.sequence,
            ByteSize(self.packed_data.len() as u64)
        )
    }
}

#[derive(Error, Debug)]
pub enum ExtractError {
    #[error("index out of bounds: id {id} > dictionary section len {len}")]
    IdOutOfBounds { id: Id, len: usize },
    #[error("read invalid UTF-8 sequence in {data:?}, recovered: '{recovered}'")]
    InvalidUtf8 { source: std::str::Utf8Error, data: Vec<u8>, recovered: String },
}

impl DictSectPFC {
    /// size in bytes of the dictionary section
    pub fn size_in_bytes(&self) -> usize {
        self.sequence.size_in_bytes() + self.packed_data.len()
    }

    fn index_str(&self, index: usize) -> &str {
        let position: usize = self.sequence.get(index);
        let length = self.strlen(position);
        str::from_utf8(&self.packed_data[position..position + length]).unwrap()
    }

    /// translated from Java
    /// <https://github.com/rdfhdt/hdt-java/blob/master/hdt-java-core/src/main/java/org/rdfhdt/hdt/dictionary/impl/section/PFCDictionarySection.java>>
    /// 0 means not found
    pub fn string_to_id(&self, element: &str) -> Id {
        if self.num_strings == 0 {
            // shared dictionary may be empty
            return 0;
        }
        // binary search
        let mut low: usize = 0;
        let mut high = self.sequence.entries.saturating_sub(2); // should be -1 but only works with -2, investigate
        let max = high;
        let mut mid = high;
        while low <= high {
            mid = usize::midpoint(low, high);

            let cmp: Ordering = if mid > max {
                mid = max;
                break;
            } else {
                let text = self.index_str(mid);
                element.cmp(text)
                //println!("mid: {} text: {} cmp: {:?}", mid, text, cmp);
            };
            match cmp {
                Ordering::Less => {
                    if mid == 0 {
                        return 0;
                    }
                    high = mid - 1;
                }
                Ordering::Greater => low = mid + 1,
                Ordering::Equal => return ((mid * self.block_size) + 1) as Id,
            }
        }
        if high < mid {
            mid = high;
        }
        let idblock = self.locate_in_block(mid, element);
        if idblock == 0 {
            return 0;
        }
        ((mid * self.block_size) + idblock + 1) as Id
    }

    fn longest_common_prefix(a: &[u8], b: &[u8]) -> usize {
        let len = min(a.len(), b.len());
        let mut delta = 0;
        while delta < len && a[delta] == b[delta] {
            delta += 1;
        }
        delta
    }

    fn locate_in_block(&self, block: usize, element: &str) -> usize {
        if block >= self.sequence.entries {
            return 0;
        }
        let element = element.as_bytes();
        let mut pos = self.sequence.get(block);
        let mut id_in_block = 0;
        let mut cshared = 0;

        // Read the first string in the block
        let slen = self.strlen(pos);
        let mut temp_string: Vec<u8> = self.packed_data[pos..pos + slen].to_vec();
        pos += slen + 1;
        id_in_block += 1;

        while (id_in_block < self.block_size) && (pos < self.packed_data.len()) {
            // Decode prefix
            let (delta, vbyte_bytes) = decode_vbyte_delta(&self.packed_data, pos);
            pos += vbyte_bytes;

            //Copy suffix
            let slen = self.strlen(pos);
            temp_string.truncate(delta);
            temp_string.extend_from_slice(&self.packed_data[pos..pos + slen]);
            if delta >= cshared {
                // Current delta value means that this string has a larger long common prefix than the previous one
                cshared += Self::longest_common_prefix(&temp_string[cshared..], &element[cshared..]);

                if (cshared == element.len()) && (temp_string.len() == element.len()) {
                    break;
                }
            } else {
                // We have less common characters than before, this string is bigger that what we are looking for.
                // i.e. Not found.
                id_in_block = 0;
                break;
            }
            pos += slen + 1;
            id_in_block += 1;
        }

        if pos >= self.packed_data.len() || id_in_block == self.block_size {
            id_in_block = 0;
        }
        id_in_block
    }

    /// extract the string with the given ID between 1 and self.num_strings (inclusive) from the dictionary
    pub fn extract(&self, id: Id) -> core::result::Result<String, ExtractError> {
        if id as usize > self.num_strings {
            return Err(ExtractError::IdOutOfBounds { id, len: self.num_strings });
        }
        let block_index = id.saturating_sub(1) as usize / self.block_size;
        let string_index = id.saturating_sub(1) as usize % self.block_size;
        let mut position = self.sequence.get(block_index);
        let mut slen = self.strlen(position);
        let mut string: Vec<u8> = self.packed_data[position..position + slen].to_vec();
        //println!("block_index={} string_index={}, string={}", block_index, string_index, str::from_utf8(&string).unwrap());
        // loop takes around nearly half the time of the function
        for _ in 0..string_index {
            position += slen + 1;
            let (delta, vbyte_bytes) = decode_vbyte_delta(&self.packed_data, position);
            position += vbyte_bytes;
            slen = self.strlen(position);
            string.truncate(delta);
            string.extend_from_slice(&self.packed_data[position..position + slen]);
        }
        // tried simdutf8::basic::from_utf8 but that didn't speed up extract that much
        match str::from_utf8(&string) {
            Ok(string) => Ok(String::from(string)),
            Err(e) => Err(ExtractError::InvalidUtf8 {
                source: e,
                data: string.clone(),
                recovered: String::from_utf8_lossy(&string).into_owned(),
            }),
        }
    }

    fn strlen(&self, offset: usize) -> usize {
        self.packed_data[offset..].iter().position(|&b| b == 0).unwrap_or(self.packed_data.len() - offset)
    }

    /// deprecated: we should be able to remove this as it is public now
    pub const fn num_strings(&self) -> usize {
        self.num_strings
    }

    /// Common parsing logic for reading a dictionary section.
    /// Returns the parsed components and the CRC32 code to be verified.
    fn read_internal<R: BufRead>(reader: &mut R) -> Result<ReadInternalResult> {
        // https://www.rdfhdt.org/hdt-binary-format/#DictionarySection specifies "unsigned 32bit value preamble" but 8bit is used in practice
        let mut preamble = [0_u8];
        reader.read_exact(&mut preamble)?;
        if preamble[0] != 2 {
            return Err(Error::DictSectNotPfc(preamble[0]));
        }

        // read section meta data
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest8 = crc8.digest();
        // The CRC includes the type of the block, inaccuracy in the spec, careful.
        digest8.update(&[0x02]);
        // This was determined based on https://git.io/JthMG because the spec on this
        // https://www.rdfhdt.org/hdt-binary-format was inaccurate, it's 3 vbytes, not 2.
        let (num_strings, bytes_read) = read_vbyte(reader)?;
        digest8.update(&bytes_read);
        //println!("num strings {num_strings}");
        let (packed_length, bytes_read) = read_vbyte(reader)?;
        digest8.update(&bytes_read);
        //println!("packed_length {packed_length}");
        let (block_size, bytes_read) = read_vbyte(reader)?;
        digest8.update(&bytes_read);
        //println!("block_size {block_size}");
        // read section CRC8
        let mut crc_code8 = [0_u8];
        reader.read_exact(&mut crc_code8)?;
        let crc_code8 = crc_code8[0];
        //println!("crc_code {crc_code8}");

        let crc_calculated8 = digest8.finalize();
        if crc_calculated8 != crc_code8 {
            return Err(Error::InvalidCrc8Checksum(crc_calculated8, crc_code8));
        }
        // read sequence log array
        let sequence = Sequence::read(reader)?;
        //println!("read sequence of length {} {:?}", sequence.data.len(), sequence.data);

        // read packed data
        let mut packed_data = vec![0u8; packed_length];
        reader.read_exact(&mut packed_data)?;
        let packed_data = Arc::<[u8]>::from(packed_data);
        //println!("read packed data of length {} {:?}", packed_data.len(), packed_data);

        // read packed data CRC32
        let mut crc_code = [0u8; 4];
        reader.read_exact(&mut crc_code)?;

        Ok((num_strings, block_size, sequence, packed_data, crc_code))
    }

    /// Verifies the CRC32 checksum and constructs the DictSectPFC.
    fn verify_and_construct(
        num_strings: usize, block_size: usize, sequence: Sequence, packed_data: Arc<[u8]>, crc_code: [u8; 4],
    ) -> Result<Self> {
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest32 = crc32.digest();
        digest32.update(&packed_data[..]);
        let crc_calculated32 = digest32.finalize();
        let crc_code32 = u32::from_le_bytes(crc_code);
        if crc_calculated32 != crc_code32 {
            return Err(Error::InvalidCrc32Checksum(crc_calculated32, crc_code32));
        }
        Ok(DictSectPFC { num_strings, block_size, sequence, packed_data })
    }

    /// Returns an unverified dictionary section together with a handle to verify the checksum.
    #[cfg(not(any(target_arch = "wasm32", target_arch = "wasm64")))]
    pub fn read<R: BufRead>(reader: &mut R) -> Result<JoinHandle<Result<Self>>> {
        let (num_strings, block_size, sequence, packed_data, crc_code) = Self::read_internal(reader)?;
        Ok(spawn(move || Self::verify_and_construct(num_strings, block_size, sequence, packed_data, crc_code)))
    }

    /// WASM-specific version that returns result directly without threading
    #[cfg(any(target_arch = "wasm32", target_arch = "wasm64"))]
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self> {
        let (num_strings, block_size, sequence, packed_data, crc_code) = Self::read_internal(reader)?;
        Self::verify_and_construct(num_strings, block_size, sequence, packed_data, crc_code)
    }

    /// counterpoint to the read method
    pub fn write(&self, dest_writer: &mut impl Write) -> Result<()> {
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest8 = crc8.digest();
        // libhdt/src/libdcs/CSD_PFC.cpp::save()
        // save type
        let seq_type: [u8; 1] = [2];
        dest_writer.write_all(&seq_type)?;
        digest8.update(&seq_type);

        // // Save sizes
        let mut buf: Vec<u8> = vec![];
        buf.extend_from_slice(&encode_vbyte(self.num_strings));
        buf.extend_from_slice(&encode_vbyte(self.packed_data.len()));
        buf.extend_from_slice(&encode_vbyte(self.block_size));
        dest_writer.write_all(&buf)?;
        digest8.update(&buf);
        let checksum8: u8 = digest8.finalize();
        dest_writer.write_all(&[checksum8])?;

        self.sequence.write(dest_writer)?;

        // Write packed data
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest32 = crc32.digest();
        dest_writer.write_all(&self.packed_data)?;
        digest32.update(&self.packed_data);
        // println!("{}", String::from_utf8_lossy(&self.compressed_terms));
        let checksum32 = digest32.finalize();
        let checksum_bytes: [u8; 4] = checksum32.to_le_bytes();
        //println!("write crc32 {checksum_bytes:?}");
        dest_writer.write_all(&checksum_bytes)?;
        dest_writer.flush()?;
        Ok(())
    }

    /// Number of front coded blocks in this section.
    pub const fn num_blocks(&self) -> usize {
        if self.num_strings == 0 || self.block_size == 0 { 0 } else { self.num_strings.div_ceil(self.block_size) }
    }

    /// Decodes the strings of the given blocks in order, calling `f` with the zero based index of
    /// each string in this section (its ID minus one) and the string itself.
    /// Unlike calling [`Self::extract`] per ID, every block is walked once, with one reused buffer.
    /// Maplib addition: lets the dictionary be decoded in parallel by splitting the block range.
    pub fn decode_blocks(&self, blocks: std::ops::Range<usize>, mut f: impl FnMut(usize, &str)) {
        let mut buf: Vec<u8> = Vec::new();
        for block in blocks {
            let first = block * self.block_size;
            let n = min(self.block_size, self.num_strings.saturating_sub(first));
            let mut pos = self.sequence.get(block);
            for j in 0..n {
                if j == 0 {
                    buf.clear();
                } else {
                    let (delta, vbyte_bytes) = decode_vbyte_delta(&self.packed_data, pos);
                    pos += vbyte_bytes;
                    buf.truncate(delta);
                }
                let slen = self.strlen(pos);
                buf.extend_from_slice(&self.packed_data[pos..pos + slen]);
                pos += slen + 1;
                match str::from_utf8(&buf) {
                    Ok(s) => f(first + j, s),
                    Err(_) => f(first + j, &String::from_utf8_lossy(&buf)),
                }
            }
        }
    }

    /// Compress sorted, unique terms from a `BTreeSet` into a PFC section.
    pub fn compress(terms: &BTreeSet<&str>, block_size: usize) -> Self {
        Self::compress_iter(terms.iter().copied(), terms.len(), block_size)
    }

    /// Compress pre-sorted, unique terms into a PFC section.
    ///
    /// The caller must guarantee `terms` yields exactly `num_terms` items in
    /// ascending lexicographic order with no duplicates. This entry point lets
    /// callers avoid materializing an intermediate `BTreeSet` or `Vec<&str>`
    /// when they already have the sorted sequence (e.g. a sorted `Vec<u32>` of
    /// term indices resolved on the fly) — the major memory saver during NT ingest.
    pub fn compress_iter<'a, I>(terms: I, num_terms: usize, block_size: usize) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut compressed_terms = Vec::new();
        let mut offsets = Vec::new();
        let mut last_term: &[u8] = &[];

        for (i, term) in terms.into_iter().enumerate() {
            let term_bytes: &[u8] = term.as_bytes();
            if i % block_size == 0 {
                offsets.push(compressed_terms.len());
                compressed_terms.extend_from_slice(term_bytes);
            } else {
                let common_prefix_len = last_term.iter().zip(term_bytes).take_while(|(a, b)| a == b).count();
                compressed_terms.extend_from_slice(&encode_vbyte(common_prefix_len));
                compressed_terms.extend_from_slice(&term_bytes[common_prefix_len..]);
            }

            compressed_terms.push(0); // Null separator
            last_term = term_bytes;
        }
        if num_terms > 0 {
            offsets.push(compressed_terms.len());
        }

        // offsets are an increasing list of array indices, therefore the last one will be the largest
        // TODO: potential off by 1 in comparison with hdt-cpp implementation?
        //let bits_per_entry = if num_terms == 0 { 0 } else { (offsets.last().unwrap().ilog2() + 1) as usize };
        DictSectPFC {
            num_strings: num_terms,
            block_size,
            //sequence: Sequence::new(&offsets, bits_per_entry),
            sequence: Sequence::new(&offsets),
            packed_data: Arc::from(compressed_terms),
        }
    }
}
