use std::io;
use std::io::BufRead;

const MAX_VBYTE_BYTES: usize = usize::BITS as usize / 7 + 1;

/// little endian
pub fn read_vbyte<R: BufRead>(reader: &mut R) -> io::Result<(usize, Vec<u8>)> {
    use io::Error;
    use io::ErrorKind::InvalidData;

    let mut n: u128 = 0;
    let mut shift = 0;
    let mut buffer = [0u8];
    let mut bytes_read = Vec::new();
    reader.read_exact(&mut buffer)?;
    bytes_read.extend_from_slice(&buffer);

    while (buffer[0] & 0x80) == 0 {
        if bytes_read.len() >= MAX_VBYTE_BYTES {
            return Err(Error::new(InvalidData, "Tried to read a VByte that does not fit into a usize"));
        }

        n |= ((buffer[0] & 127) as u128) << shift;
        reader.read_exact(&mut buffer)?;
        bytes_read.extend_from_slice(&buffer);
        // IMPORTANT: The original implementation has an off-by-one error here, hence we
        // have to copy the same off-by-one error in order to read the file format.
        // The correct implementation is supposed to shift by 8! Look at the commented out
        // tests at the bottom of the file for proof.
        shift += 7;
    }

    n |= ((buffer[0] & 127) as u128) << shift;

    usize::try_from(n).map_or_else(
        |_| Err(Error::new(InvalidData, "Tried to read a VByte that does not fit into a usize")),
        |valid| Ok((valid, bytes_read)),
    )
}

/// decode vbyte with offset
pub const fn decode_vbyte_delta(data: &[u8], offset: usize) -> (usize, usize) {
    let mut n: usize = 0;
    let mut shift: usize = 0;
    let mut byte_amount = 0;

    while (data[offset + byte_amount] & 0x80) == 0 {
        n |= ((data[offset + byte_amount] & 127) as usize) << shift;
        byte_amount += 1;
        shift += 7;
    }

    n |= ((data[offset + byte_amount] & 127) as usize) << shift;
    byte_amount += 1;

    (n, byte_amount)
}

/// little endian
pub fn encode_vbyte(n: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut n = n;

    while n > 127 {
        bytes.push((n & 127) as u8);
        // IMPORTANT: The original implementation has an off-by-one error here, hence we
        // have to copy the same off-by-one error in order to read the file format.
        // The correct implementation is supposed to shift by 8! Look at the commented out
        // tests at the bottom of the file for proof.
        n >>= 7;
    }

    bytes.push((n | 0x80) as u8);
    bytes
}
