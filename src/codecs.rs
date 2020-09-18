use num_traits::{Num, NumCast, PrimInt};
use std::{marker::PhantomData, convert::TryInto};
use super::errors::*;
use crate::nibble::Nibble;

/// Helper trait allowing RLE1/2 to work for both signed and unsigned
pub trait Sign : Num + Copy + NumCast + PrimInt + std::fmt::Debug {
    fn unzigzag(value: u128) -> Self;
}
impl Sign for i128 {
    // Undo zigzag encoding
    fn unzigzag(value: u128) -> Self {
        let value = value as i128;
        (value << 127 >> 127) ^ (value >> 1)
    }
}

impl Sign for u128 {
    // Don't do anything
    fn unzigzag(value: u128) -> Self {
        value
    }
}

pub trait Decoder<'t> : std::fmt::Debug {
    type Output;
    fn remainder(&self) -> &'t [u8];
    fn next_result(&mut self) -> OrcResult<Self::Output>;
    fn read_one(&mut self) -> OrcResult<(Self::Output, &'t [u8])> {
        let n = self.next_result()?;
        Ok((n, self.remainder()))
    }
}
macro_rules! decoder_iter {
    ($dec:ty, $out:ty) => {
        impl<'t> Iterator for $dec {
            type Item = OrcResult<$out>;
            fn next(&mut self) -> Option<Self::Item> {
                match self.next_result() {
                    Ok(x) => Some(Ok(x)),
                    Err(OrcError::EndOfStream) => None,
                    Err(x) => Some(Err(x))
                }
            }
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
decoder_iter!(ByteRLEDecoder<'t>, u8);
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

/// 32 bit float storage. It's not so much an encoding as just an array.
#[derive(Debug)]
pub struct FloatDecoder<'t> {
    buf: &'t [u8]
}
decoder_iter!(FloatDecoder<'t>, f32);
impl<'t> From<&'t[u8]> for FloatDecoder<'t> {
    fn from(buf: &'t[u8]) -> Self {
        FloatDecoder { buf }
    }
}
impl<'t> Decoder<'t> for FloatDecoder<'t> {
    type Output = f32;
    fn remainder(&self) -> &'t [u8] {
        self.buf
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        if self.buf.len() < 4 {
            Err(OrcError::EndOfStream)
        } else {
            let f = f32::from_be_bytes(self.buf[..4].try_into().unwrap());
            self.buf = &self.buf[4..];
            Ok(f)
        }
    }
}

/// 64 bit float storage. It's not so much an encoding as just an array.
#[derive(Debug)]
pub struct DoubleDecoder<'t> {
    buf: &'t [u8]
}
decoder_iter!(DoubleDecoder<'t>, f64);
impl<'t> From<&'t[u8]> for DoubleDecoder<'t> {
    fn from(buf: &'t[u8]) -> Self {
        DoubleDecoder { buf }
    }
}
impl<'t> Decoder<'t> for DoubleDecoder<'t> {
    type Output = f64;
    fn remainder(&self) -> &'t [u8] {
        self.buf
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        if self.buf.len() < 8 {
            Err(OrcError::EndOfStream)
        } else {
            let f = f64::from_be_bytes(self.buf[..8].try_into().unwrap());
            self.buf = &self.buf[8..];
            Ok(f)
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
decoder_iter!(BooleanRLEDecoder<'t>, bool);
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
pub struct VarDecoder<'t, S: Sign> {
    buf: &'t [u8],
    ghost: PhantomData<S>
}
decoder_iter!(VarDecoder<'t, i128>, i128);
decoder_iter!(VarDecoder<'t, u128>, u128);
impl<'t, S: Sign> From<&'t[u8]> for VarDecoder<'t, S> {
    fn from(buf: &'t[u8]) -> Self {
        VarDecoder {buf, ghost: PhantomData}
    }
}
impl<'t, S: Sign> Decoder<'t> for VarDecoder<'t, S> {
    type Output = S;
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
                return Ok(S::unzigzag(value));
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

/// Integer RLE compatible with Hive < 0.12
#[derive(Debug)]
pub struct RLE1<'t, S: Sign> {
    buf: VarDecoder<'t, S>,
    run: isize,
    literal: isize,
    run_item: S,
    delta: S,
    ghost: PhantomData<S>
}
decoder_iter!(RLE1<'t, u128>, u128);
decoder_iter!(RLE1<'t, i128>, i128);
impl<'t, S: Sign> From<&'t [u8]> for RLE1<'t, S> {
    fn from(buf: &'t[u8]) -> Self {
        RLE1{buf: buf.into(), run:0, literal: 0, run_item: S::unzigzag(0), delta: S::unzigzag(0), ghost: PhantomData}
    }
}
/// Most of the RLE implementations look similar. Look at the ByteRLE for the clearest example
impl<'t, S: Sign> Decoder<'t> for RLE1<'t, S> {
    type Output = S;
    fn remainder(&self) -> &'t [u8] {
        self.buf.remainder()
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        if self.run > 0 {
            // Return this value, and then increment it
            self.run -= 1;
            let next = self.run_item;
            self.run_item = self.run_item.add(self.delta);
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
                self.delta = S::unzigzag(*delta as u128);
                self.run_item = self.buf.next_result().map_err(|err| match err {
                    OrcError::EndOfStream => OrcError::TruncatedError("IntRLE1 missing run item"),
                    x => x
                })?;
            }
            self.next_result()
        }
    }
}

/// Integer RLE compatible with Hive < 0.12
///
/// This is the most complicated format in ORC
#[derive(Debug)]
pub struct RLE2<'t, Inner> {
    nib: Nibble<'t>,
    mode: RLE2Mode<'t,Inner>,
    width: usize,
    length: usize,
    ghost: std::marker::PhantomData<Inner>
}

/// RLE2 switches between four modes of operation.
/// The width and length are always necessary so they are in the encoder
#[derive(Debug)]
enum RLE2Mode<'t, Inner> {
    ShortRepeat(Inner),
    Direct,
    PatchedBase{
        base: Inner,
        patch_width: usize,
        patch_gap_width: usize,
        remaining_patches: usize,
        remaining_patch_gap: usize,
        patch_buffer: Nibble<'t>
    },
    Delta {
        // See comments in get_mode on why base is i128
        base: i128,
        first_delta: i128,
        consumed: usize
    }
}
decoder_iter!(RLE2<'t, u128>, u128);
decoder_iter!(RLE2<'t, i128>, i128);
impl<'t, S: Sign> From<&'t [u8]> for RLE2<'t, S> {
    fn from(buf: &'t[u8]) -> Self {
        RLE2{
            nib: Nibble{buf: buf.into(), start: 0},
            mode: RLE2Mode::Direct,
            width: 0,
            length: 0,
            ghost: std::marker::PhantomData
        }
    }
}
impl<'t, S: Sign> RLE2<'t, S> {

    /// Decode the bit widths as they are stored in 5 bits
    fn decode_width(&mut self) -> OrcResult<usize> {
        // Convert encoded to decoded widths for certain modes
        // I have no idea why these are out of order
        #[rustfmt::skip]
        let bit_width_encoding = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            // Inputs 22 and 23 are not valid but we'll continue the pattern
            22, 23,
            // 24 is valid and keeps the patterm
            24,
            // The rest break it
            26, 28, 30, 32, 40, 48, 56, 64
        ];
        let width_before : usize = self.nib.read(5, "RLE2 header width")?;
        Ok(bit_width_encoding[width_before])
    }

    /// Start a patch, choosing a mode for interpreting the upcoming string
    fn get_mode(&mut self) -> OrcResult<()> {
        if self.nib.is_end() {
            return Err(OrcError::EndOfStream);
        }
        // The header can be 1, 2, or 4 bytes.
        // The most significant 2 bits of the first byte will tell you how long the header is.

        match self.nib.read(2, "RLE2 encoding type")? {
            0 => {
                // The first byte looks like EEWWWLLL
                // where E, W, L = encoding, width, length
                self.width = self.nib.read::<usize>(3, "RLE2 short read width")? + 1;
                self.length = self.nib.read::<usize>(3, "RLE2 short repeat length")? + 3;
                let elem = self.nib.read(self.width*8, "RLE2 short repeat element")?;
                
                self.mode = RLE2Mode::ShortRepeat(elem);
            },
            1 => {
                // The first two bytes look like EEWWWWWL LLLLLLLL
                // where E, W, L = encoding, width, length
                self.width = self.decode_width()?;
                self.length = self.nib.read::<usize>(9, "RLE2 header length")? + 1;
                self.mode = RLE2Mode::Direct;
            },
            2 => {
                // The first four bytes look like EEWWWWWL LLLLLLLL BBBPPPPP GGGNNNNN
                // where E, W, L = encoding, width, length
                // and B = base value width
                // and P = patch value width
                // and G = patch gap width
                // and N = patch list length

                // Width and Length are the same as the direct encoding, in bits.
                self.width = self.decode_width()?;
                self.length = self.nib.read::<usize>(9, "RLE2 header length")? + 1;

                // The rest are extra parameters not found in the other modes
                let base_width = self.nib.read::<usize>(3, "RLE2 patched base: base width")? + 1;
                let patch_width = self.decode_width()?;
                let patch_gap_width = self.nib.read::<usize>(3, "RLE2 patched base: patch gap width")? + 1;
                let remaining_patches = self.nib.read(5, "RLE2 patched base: patch list length")?; // can be 0

                // NOTE: The spec seems ambiguous here:
                // > The base value is stored as a big endian value with negative values marked by the most
                // > significant bit set. If it that bit is set, the entire value is negated.
                // It's not clear if this applies to unsigned numbers or if zigzag is not used for these
                // TODO: get more examples of this
                //let negate = self.nib.read(1, "RLE2 negate bit")?;
                let base: u128 = self.nib.read(base_width * 8, "RLE2 patched base: base")?;

                let mut patch_buffer = Nibble {
                    buf: self.nib.buf,
                    start: (self.nib.start + (self.width * self.length)) + 7 / 8
                };
                let remaining_patch_gap = patch_buffer.read(patch_gap_width, "RLE2 initial patch gap")?;

                self.mode = RLE2Mode::PatchedBase{
                    base: S::unzigzag(base),
                    patch_width,
                    patch_gap_width,
                    remaining_patches,
                    remaining_patch_gap,
                    patch_buffer 
                };
            },
            3 => {
                // The first two bytes look like EEWWWWWL LLLLLLLL
                // where E, W, L = encoding, width, length
                self.width = self.decode_width()?;
                self.length = self.nib.read::<usize>(9, "RLE2 header length")? + 1;

                // Deltas aren't valid for a sequence of less than two items, so the length starts at two.
                // Note here that base can be signed or unsigned (so we need the conditional unzigzag)
                // but the delta is always signed.
                // So even though we may choose to unzigzag base or not, we will still store it as i128,
                // so that we can apply the deltas.
                // Then we will do a no-op conversion later to get either u128 or i128 as necessary.
                //
                // Unlike the other modes of RLE2 these are legit varints with MSB flags like protobuf uses
                let base: S = self.nib.byte_level_interlude(|x| VarDecoder::from(x).read_one())?;
                let first_delta = self.nib.byte_level_interlude(|x| VarDecoder::from(x).read_one())?;
                self.mode = RLE2Mode::Delta {
                    // Everything should fit in an i128
                    base: NumCast::from(base).unwrap(),
                    first_delta,
                    consumed: 0
                }
            },
            _ => unreachable!() 
        }
        Ok(())
    }
}
/// Most of the RLE implementations look similar. Look at the ByteRLE for the clearest example
impl<'t, S: Sign> Decoder<'t> for RLE2<'t, S> {
    type Output = S;
    fn remainder(&self) -> &'t [u8] {
        self.nib.remainder()
    }
    fn next_result(&mut self) -> OrcResult<Self::Output> {
        if self.length == 0 {
            self.get_mode()?;
        }
        self.length = self.length.saturating_sub(1);
        match self.mode {
            RLE2Mode::ShortRepeat(item) => {
                Ok(item) 
            }
            RLE2Mode::Direct => {
                self.nib.read(self.width, "RLE2 direct encoded int")
            }
            RLE2Mode::PatchedBase{
                base,
                patch_width,
                patch_gap_width,
                ref mut remaining_patches,
                ref mut remaining_patch_gap,
                ref mut patch_buffer
            } => {
                let mut value = base;
                value = value + self.nib.read(self.width, "RLE2 patched base delta")?;
                if *remaining_patch_gap == 0 && *remaining_patches > 0 {
                    // The patch (if present) must be applied before the addition
                    value = value + (patch_buffer.read::<S>(patch_width, "RLE2 patched base: patch")? << self.width);
                    *remaining_patches -= 1;
                    if *remaining_patches > 0 {
                        *remaining_patch_gap = patch_buffer.read(patch_gap_width, "RLE2 patched base: patch gap")?;
                    }
                }
                *remaining_patch_gap = remaining_patch_gap.saturating_sub(1);
                // We need the main nibble to catch up to the patch nibble when the patched base mode is done
                if self.length == 0 {
                    std::mem::swap(&mut self.nib,  patch_buffer);
                    self.nib.round_up();
                }
                Ok(value)
            }
            RLE2Mode::Delta{ref mut base, first_delta, ref mut consumed} => {
                match *consumed {
                    0 => (),
                    1 => *base += first_delta, 
                    _ => *base +=
                        first_delta.signum()
                        * self.nib.read::<i128>(self.width, "RLE2 delta: delta")?
                };
                *consumed += 1;
                NumCast::from(*base).ok_or(OrcError::LongBitstring)
            }
        }
    }
}

