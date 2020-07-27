use super::errors::*;
#[derive(Debug)]
pub enum PrimitiveStream<'t> {
    Boolean(BooleanRLEDecoder<'t>),
    Byte(ByteRLEDecoder<'t>)
}

/// Basic over the wire types, all primitives with a type-specific compression method
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum Codec {
    /// Boolean run length encoding
    BooleanRLE,
    /// One-byte signed integer run length encoding
    ByteRLE,
    /// Variable width signed integer run length encoding (for Hive < 0.12)
    IntRLE1,
    /// Variable width signed integer run length encoding (for Hive >= 0.12)
    IntRLE2,
    /// Variable width unsigned integer run length encoding (for Hive < 0.12)
    UintRLE1,
    /// Variable width unsigned integer run length encoding (for Hive >= 0.12)
    UintRLE2,
    /// Fixed width floating point IEEE754. Essentially raw.
    FloatEnc,
    /// Raw uncompressed slice, no encoding
    BinaryEnc,
    /// Integer encoding that mangles trailing zeros to reduce magnitude (for Hive < 0.12)
    NanosEnc1,
    /// Integer encoding that mangles trailing zeros to reduce magnitude (for Hive >= 0.12)
    NanosEnc2
}

impl Codec {
    /// Initialize the appropriate decoder for this encoding
    pub fn decode<'t>(&self, buf: &'t [u8]) -> OrcResult<PrimitiveStream<'t>> {
        Ok(match self {
            Codec::BooleanRLE => PrimitiveStream::Boolean(BooleanRLEDecoder::from(buf)),
            Codec::ByteRLE => PrimitiveStream::Byte(ByteRLEDecoder::from(buf)),
            _ => todo!("Codec not implemented")
        })
    }
}

pub trait Decoder<'t> {
    type Output;
    fn remainder(&self) -> &'t [u8];
    fn next_result(&mut self) -> OrcResult<Self::Output>;
}
impl<'t, Out> Iterator for dyn Decoder<'t, Output=Out> {
    type Item = OrcResult<Out>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.next_result() {
            Ok(x) => Some(Ok(x)),
            Err(OrcError::EndOfStream) => None,
            Err(x) => Some(Err(x))
        }
    }
}

/// Byte level RLE, encoding up to 128 byte literals and up to 130 byte runs
/// It works pretty much like it sounds, a flag followed by a string (for literals),
/// or by a single byte (for runs)
#[derive(Debug)]
pub struct ByteRLEDecoder<'t> {
    buf: &'t [u8],
    run: isize,
    run_item: u8,
    literal: isize
}
impl<'t> From<&'t[u8]> for ByteRLEDecoder<'t> {
    fn from(buf: &'t[u8]) -> Self {
        ByteRLEDecoder {buf, run:0, literal:0, run_item:0}
    }
}
impl<'t> Decoder<'t> for ByteRLEDecoder<'t> {
    type Output = u8;
    fn remainder(&self) -> &'t [u8] {
        self.buf
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        if self.run > 0 {
            // Repeat the last byte
            self.run -= 1;
            Ok(self.run_item)
        } else if self.literal > 0 {
            // Consume a byte
            match self.buf.first() {
                // Can't run out inside a literal
                None => Err(OrcError::TruncatedError("ByteRLE literal cut short")),
                Some(first) => {
                    self.literal -= 1;
                    self.buf = &self.buf[1..];
                    Ok(*first)
                }
            }
        } else if self.buf.is_empty() {
            Err(OrcError::EndOfStream)
        } else if self.buf.len() == 1 {
            // If we have at least two bytes, we're ok.
            // If we had zero bytes, we already handled that above.
            // But if we have one byte, we must have ended on a control byte
            Err(OrcError::TruncatedError("Ended ByteRLE on a control byte"))
        } else {
            // Start the next run
            let flag = self.buf[0] as i8;
            if flag < 0 {
                self.literal = -(flag as isize);
                self.buf = &self.buf[1..];
            } else {
                // The minimum run size is 3, so they hard coded that
                self.run = 3 + (flag as isize);
                self.run_item = self.buf[1];
                self.buf = &self.buf[2..];
            }
            self.next_result()
        }
    }
}

/// Boolean encoder based on byte-level RLE
///
/// There is concerning ambiguity about the length of a boolean RLE.
/// It's always padded to a multiple of 8, so you will likely get extra elements.
#[derive(Debug)]
pub struct BooleanRLEDecoder<'t> {
    buf: ByteRLEDecoder<'t>,
    bit: usize,
    byte: u8
}
impl<'t> From<&'t[u8]> for BooleanRLEDecoder<'t> {
    fn from(buf: &'t[u8]) -> Self {
        BooleanRLEDecoder {
            buf: ByteRLEDecoder::from(buf),
            bit: 0,
            byte: 0
        }
    }
}
impl<'t> Decoder<'t> for BooleanRLEDecoder<'t> {
    type Output = bool;
    fn remainder(&self) -> &'t [u8] {
        self.buf.remainder()
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        if self.bit == 0 {
            // Try to advance to the next byte
            self.byte = self.buf.next_result()?;
            self.bit = 8;
        }
        self.bit -= 1;
        Ok((self.byte & (1 << self.bit)) != 0)
    }
}
/// Unsigned varint128 decoder, compatible with protobuf
#[derive(Debug)]
pub struct VaruintDecoder<'t> {
    buf: &'t [u8]
}
impl<'t> From<&'t[u8]> for VaruintDecoder<'t> {
    fn from(buf: &'t[u8]) -> Self {
        VaruintDecoder {buf}
    }
}
impl<'t> Decoder<'t> for VaruintDecoder<'t> {
    type Output = u128;
    fn remainder(&self) -> &'t [u8] {
        self.buf
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        let mut value = 0;
        for i in 0..self.buf.len() {
            // Each consecutive byte reads from least to most significant in the value we're building
            let byte = (self.buf[i] & 0x7F) as u128;
            // Keep in mind the last byte will overflow the 128bit int by 5 bits
            // because 7 * 18 = 126, and the MSB will still be taken out of the last byte, so that leaves 5 extra.
            // The docs don't mention what happens with those bits. We're gambling that they are zeros
            // but to be pedantic you could add a conditional to check the length. Hopefully that's not necessary.
            value |= byte.wrapping_shl(7*i as u32);
            if self.buf[i] & 0x80 == 0 {
                // The most significant bit in each byte (0x80) indicates if the integer continues in the next byte
                self.buf = &self.buf[i+1..];
                return Ok(value);
            }
        }
        if self.buf.is_empty() {
            Err(OrcError::EndOfStream)
        } else {
            // We must have run out of buffer and triggered for{} to stop but the MSB said there was still more.
            Err(OrcError::TruncatedError("Decoding varint128"))
        }
    }
}
/// Signed Varint128 decoder using zigzag, compatible with protobuf.
///
/// This is a super trivial extension to unsigned varint
#[derive(Debug)]
pub struct VarintDecoder<'t>{
    buf: VaruintDecoder<'t>
}
impl<'t> From<&'t[u8]> for VarintDecoder<'t> {
    fn from(buf: &'t[u8]) -> Self {
        VarintDecoder{buf: buf.into()}
    }
}
impl<'t> Decoder<'t> for VarintDecoder<'t> {
    type Output = i128;
    fn remainder(&self) -> &'t [u8] {
        self.buf.remainder()
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        let value = self.buf.next_result()? as i128;
        // Inverse of (n << 1) ^ (n >> 127)
        // The left and right shift will clear all but one bit. There may be a better way.
        Ok((value << 127 >> 127) ^ (value >> 1))
    }
}

/// Integer RLE compatible with Hive < 0.12
#[derive(Debug)]
pub struct UintRLE1<'t> {
    buf: VaruintDecoder<'t>,
    run: isize,
    literal: isize,
    run_item: u128,
    delta: u128
}
impl<'t> From<&'t [u8]> for UintRLE1<'t> {
    fn from(buf: &'t[u8]) -> Self {
        UintRLE1{buf: buf.into(), run:0, literal: 0, run_item: 0, delta:0}
    }
}
/// Most of the RLE implementations look similar. Look at the ByteRLE for the clearest example
impl<'t> Decoder<'t> for UintRLE1<'t> {
    type Output = u128;
    fn remainder(&self) -> &'t [u8] {
        self.buf.remainder()
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        if self.run > 0 {
            // Return this value, and then increment it
            self.run -= 1;
            let next = self.run_item;
            self.run_item += self.delta;
            Ok(next)
        } else if self.literal > 0 {
            // Consume an int
            self.literal -= 1;
            self.buf.next_result().map_err(|err| match err {
                OrcError::EndOfStream => OrcError::TruncatedError("IntRLE1 literal cut short"),
                x => x
            })
        } else {
            // Start the next run
            let (flag, rest) = self.remainder().split_first().ok_or(OrcError::EndOfStream)?;
            let flag = *flag as i8;
            if flag < 0 {
                self.literal = -(flag as isize);
                self.buf = rest.into();
            } else {
                // The minimum run size is 3, so they hard coded that
                self.run = 3 + (flag as isize);
                let (delta, rest) = rest.split_first().ok_or(OrcError::TruncatedError("IntRLE1 missing run delta"))?;
                self.buf = rest.into();
                self.delta = *delta as u128;
                self.run_item = self.buf.next_result().map_err(|err| match err {
                    OrcError::EndOfStream => OrcError::TruncatedError("IntRLE1 missing run item"),
                    x => x
                })?;
            }
            self.next_result()
        }
    }
}

/// Signed Varint128 decoder with RLE, compatible with Hive < 0.12
///
/// This is a super trivial extension to unsigned varint
#[derive(Debug)]
pub struct IntRLE1<'t>{
    buf: UintRLE1<'t>
}
impl<'t> From<&'t[u8]> for IntRLE1<'t> {
    fn from(buf: &'t[u8]) -> Self {
        IntRLE1{buf: buf.into()}
    }
}
impl<'t> Decoder<'t> for IntRLE1<'t> {
    type Output = i128;
    fn remainder(&self) -> &'t [u8] {
        self.buf.remainder()
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        let value = self.buf.next_result()? as i128;
        // Inverse of (n << 1) ^ (n >> 127)
        // The left and right shift will clear all but one bit. There may be a better way.
        Ok((value << 127 >> 127) ^ (value >> 1))
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::*;
    use super::*;

    #[test]
    fn test_byte_rle_decoder() {
        let mut encoded_test: ByteRLEDecoder = hex!("FC DEAD BEEF 03 00 FC CAFE BABE")[..].into();
        let dec: OrcResult<Vec<u8>> = (&mut encoded_test as &mut dyn Decoder<Output=u8>).collect();
        assert_eq!(dec.unwrap(), hex!("DEAD BEEF 00 00 00 00 00 00 CAFE BABE"));
    }

    #[test]
    fn test_bool_rle_decoder() {
        let mut encoded_test : BooleanRLEDecoder = hex!("FF 80")[..].into();
        let dec: OrcResult<Vec<bool>> = (&mut encoded_test as &mut dyn Decoder<Output=bool>).collect();
        assert_eq!(dec.unwrap(), &[true, false, false, false, false, false, false, false]);
    }

    #[test]
    fn test_varint128_decoder() {
        let mut encoded_test : VaruintDecoder = hex!("
            96 01    96 01    00
            FF FF FF FF FF FF FF FF FF FF FF FF FF FF FF FF FF FF 03")[..].into();
        let dec: OrcResult<Vec<u128>> = (&mut encoded_test as &mut dyn Decoder<Output=u128>).collect();
        assert_eq!(dec.unwrap(), &[150, 150, 0, 340282366920938463463374607431768211455]);
    }

    #[test]
    fn test_signed_varint128_decoder() {
        let mut encoded_test : VarintDecoder = hex!("
            96 01    96 01    00 01 02 03 04")[..].into();
        let dec: OrcResult<Vec<i128>> = (&mut encoded_test as &mut dyn Decoder<Output=i128>).collect();
        assert_eq!(dec.unwrap(), &[75, 75, 0, -1, 1, -2, 2]);
    }

    #[test]
    fn test_intrle1_decoder() {
        let mut encoded_test : UintRLE1 = hex!("03 00 07     FB 02 03 04 07 0B")[..].into();
        let dec: OrcResult<Vec<u128>> = (&mut encoded_test as &mut dyn Decoder<Output=u128>).collect();
        assert_eq!(dec.unwrap(), &[7,7,7,7,7,7,2,3,4,7,11]);

        let mut encoded_test : IntRLE1 = hex!("03 00 07     FB 02 03 04 07 0B")[..].into();
        let dec: OrcResult<Vec<i128>> = (&mut encoded_test as &mut dyn Decoder<Output=i128>).collect();
        assert_eq!(dec.unwrap(), &[-4,-4,-4,-4,-4,-4,1,-2,2,-4,-6]);
    }
}