mod errors;
pub use crate::errors::{OrcError, OrcResult};
use prost::Message;

// Include the `messages` module, which is generated from orc.proto.
pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/orc.proto.rs"));
}

/// Read the PostScript, the bootstrap of the ORC
///
/// The postscript ends with a one-byte length at the end of the file.
/// You read that many bytes (plus one) from the end of the file and decode it.
/// The postscript contains the footer length, so next after this you should
/// read the whole footer to learn about the stripes.
pub fn read_postscript(byt: &[u8]) -> OrcResult<messages::PostScript> {
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
    use super::read_postscript;
    use crate::messages;
    #[test]
    fn test_read_postscript() {
        let orc_bytes = include_bytes!("sample.orc");
        let ps = read_postscript(orc_bytes).unwrap();
        assert_eq!(ps.magic(), "ORC");
        assert_eq!(ps.footer_length(), 584);
        assert_eq!(ps.compression(), messages::CompressionKind::Zlib);
        assert_eq!(ps.compression_block_size(), 131072);
        assert_eq!(ps.writer_version(), 4);
        assert_eq!(ps.metadata_length(), 331);
        assert_eq!(ps.stripe_statistics_length(), 0);
    }
}
