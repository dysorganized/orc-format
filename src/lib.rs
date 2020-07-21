mod errors;
pub use crate::errors::{OrcError, OrcResult};
use prost::Message;
use std::io::{Read, Seek, SeekFrom, Cursor};

// Include the `messages` module, which is generated from orc.proto.
pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/orc.proto.rs"));
}
/// A handle on an open ORC file
///
/// This object holds a reference to the remainder of the file,
/// because it has at this point only deserialized the table of contents.
/// Reading a stripe will incur further IO and hence can fail.
#[derive(Debug)]
pub struct ORCFile<F: Read+Seek> {
    metadata: messages::Metadata,
    footer: messages::Footer,
    postscript: messages::PostScript,
    file: F
}

impl<F: Read+Seek> ORCFile<F> {
    /// Read a compressed message from the ORC file
    fn read_message<M: Message+Default>(
        &mut self,
        start: usize, // Message start relative to the file start
        end: usize    // Message end (exclusive) to the file end
    ) -> OrcResult<M> {
        // Read the compressed data from the file first
        self.file.seek(SeekFrom::Start(start as u64))?;
        let mut comp_buffer = vec![0u8; end-start];
        self.file.read_exact(&mut comp_buffer)?;

        // Start decompressing
        let mut decomp_buffer= vec![];
        if comp_buffer.len() < 4 {
            // We need to guarantee there are three bytes for the length
            // but for my sanity let's assume there is at least one byte of content
            return Err(OrcError::TruncatedError);
        }
        let (chunk_len, is_compressed) = match self.postscript.compression() {
            messages::CompressionKind::None => (0, false),
            _ => {
                let enc = [comp_buffer[0], comp_buffer[1], comp_buffer[2], 0];
                let enc_len = u32::from_le_bytes(enc);
                ((enc_len / 2) as usize, (enc_len & 1 == 0))
            }
        };
        let decomp_slice = match (is_compressed, self.postscript.compression()) {
            (false, _) => {
                &comp_buffer[3..]
            }
            (_, messages::CompressionKind::Zlib) => {
                let mut decoder = flate2::read::DeflateDecoder::new(&comp_buffer[3..chunk_len+3]);
                decoder.read_to_end(&mut decomp_buffer)?;
                &decomp_buffer[..]
            }
            (_, messages::CompressionKind::Snappy) => {
                let mut decoder = snap::read::FrameDecoder::new(&comp_buffer[3..chunk_len+3]);
                decoder.read_to_end(&mut decomp_buffer)?;
                &decomp_buffer[..]
            }
            _ => todo!("Only Zlib and Snappy compression is supported yet.")
            // Snappy = 2,
            // Lzo = 3,
            // Lz4 = 4,
            // Zstd = 5,
        };
        Ok(M::decode(decomp_slice)?)
    }
    /// Read the table of contents from something readable and seekable and keep the reader
    ///
    /// It isn't finished at this point; more deserializing will be done when you read a stripe.
    /// You can pass a byte vector or a file, but streaming isn't possible because the table of contents
    /// is at the end of the file.
    pub fn from_reader(mut file: F) -> OrcResult<ORCFile<F>> {
        let file_len = file.seek(SeekFrom::End(0))? as usize;
        let buffer_len = file_len.min(275);
        if buffer_len == 0 {
            return Err(OrcError::TruncatedError)
        }
        file.seek(SeekFrom::End(-(buffer_len as i64)))?;
        let mut buffer = vec![0u8; buffer_len as usize];
        file.read_exact(&mut buffer)?;

        // Deserialize the postscript first to get the lengths of the metadata and footer
        let postscript = read_postscript(&buffer[..])?;
        let mut me = ORCFile {
            file,
            postscript,
            // read_message wants an ORCFile, but we need read_message to create the members
            // This is fine because these messages have defaults we can overwrite
            metadata: messages::Metadata::default(),
            footer: messages::Footer::default()
        };
        // The file ends with the metadata, footer, postacript, and one last byte for the postscript length
        let postscript_start = file_len - *buffer.last().unwrap() as usize - 1;
        let footer_start = postscript_start - me.postscript.footer_length() as usize;
        let metadata_start = footer_start - me.postscript.metadata_length() as usize;
        
        me.footer = me.read_message::<messages::Footer>(footer_start, postscript_start)?;
        me.metadata = me.read_message::<messages::Metadata>(metadata_start, footer_start)?;
        Ok(me)
    }
}

impl<'t> ORCFile<Cursor<&'t [u8]>> {
    /// Read an ORC file from a slice instead of an open file
    ///
    /// It isn't finished at this point; more deserializing will be done when you read a stripe.
    /// You can pass a byte vector or a file, but streaming isn't possible because the table of contents
    /// is at the end of the file.
    pub fn from_slice(byt: &'t [u8]) -> OrcResult<Self> {
        Self::from_reader(Cursor::new(byt))
    }
}

/// Read the PostScript, the bootstrap of the ORC
///
/// The postscript ends with a one-byte length at the end of the file.
/// You read that many bytes (plus one) from the end of the file and decode it.
/// The postscript contains the footer length, so next after this you should
/// read the whole footer to learn about the stripes.
pub(crate) fn read_postscript(byt: &[u8]) -> OrcResult<messages::PostScript> {
    match byt.last() {
        None => Err(OrcError::TruncatedError),
        Some(&length) => {
            let postscript_bytes = &byt[
                byt.len() - length as usize - 1 .. byt.len()-1];
            Ok(messages::PostScript::decode(postscript_bytes)?)
        }
    }
}



#[cfg(test)]
mod tests {
    use crate::messages;
    #[test]
    fn test_read_postscript() {
        let orc_bytes = include_bytes!("sample.orc");
        let ps = super::read_postscript(orc_bytes).unwrap();
        assert_eq!(ps.magic(), "ORC");
        assert_eq!(ps.footer_length(), 584);
        assert_eq!(ps.compression(), messages::CompressionKind::Zlib);
        assert_eq!(ps.compression_block_size(), 131072);
        assert_eq!(ps.writer_version(), 4);
        assert_eq!(ps.metadata_length(), 331);
        assert_eq!(ps.stripe_statistics_length(), 0);
    }

    #[test]
    fn test_read_footer() {
        let orc_bytes = include_bytes!("sample.orc");
        let orc_toc = super::ORCFile::from_slice(&orc_bytes[..]).unwrap();
    }
}
