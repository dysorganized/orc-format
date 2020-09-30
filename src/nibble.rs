use crate::errors::*;
use num_traits::NumCast;

/// Read a byte buffer bits at a time, keeping a bit-level cursor
#[derive(Debug)]
pub(crate) struct Nibble<'t> {
    pub buf: &'t [u8],
    pub start: usize,
}
impl<'t> Nibble<'t> {
    /// Is there any more to eat?
    pub(crate) fn is_end(&self) -> bool {
        self.start / 8 >= self.buf.len()
    }

    /// Round the nibble cursor to the next byte
    pub(crate) fn round_up(&mut self) {
        self.start = (self.start + 7) & !7;
    }

    /// Return the unconsumed portion of the buffer, starting at the next unread byte
    pub(crate) fn remainder(&self) -> &'t [u8] {
        &self.buf[(self.start + 7) >> 3..]
    }

    /// Read the next few bits as an unsigned integer
    ///
    /// This updates the bit_offset, which may matter to you for continuing
    pub(crate) fn read<T: NumCast>(
        &mut self,
        mut read_len: usize,
        context: &'static str,
    ) -> OrcResult<T> {
        let mut value: u64 = 0;
        if self.buf.len() < (self.start + read_len) / 8 {
            return Err(OrcError::TruncatedError(context));
        }

        while read_len > 0 {
            let (byte_index, bit_index) = (self.start >> 3, self.start & 7);
            let next_bits = read_len.min(8 - bit_index);
            let byte = self
                .buf
                .get(byte_index)
                .ok_or(OrcError::TruncatedError(context))?
                << bit_index
                >> (8 - next_bits);
            value <<= next_bits;
            value |= byte as u64;
            read_len -= next_bits;
            self.start += next_bits;
        }
        T::from(value).ok_or(OrcError::LongBitstring)
    }

    /// Run a byte level parser, like Varintdecoder, while keeping the Nibble intact
    /// This rounds up the bit reading to the next byte, and assumes the parser consumes whole bytes.
    pub(crate) fn byte_level_interlude<F, X>(&mut self, mut func: F) -> OrcResult<X>
    where
        F: FnMut(&'t [u8]) -> OrcResult<(X, &'t [u8])>,
    {
        let (x, rest) = func(self.remainder())?;
        self.buf = rest;
        self.start = 0;
        Ok(x)
    }
}
