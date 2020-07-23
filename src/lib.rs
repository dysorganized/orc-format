#[macro_use] extern crate maplit;
#[macro_use] extern crate hex_literal;
mod errors;
use prost::Message;
pub use crate::errors::{OrcError, OrcResult};
use std::io::{Read, Seek, SeekFrom, Cursor};
use std::collections::HashMap;

/// Include the `messages` module, which is generated from orc.proto.
pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/orc.proto.rs"));
}

/// A handle on an open ORC file
///
/// This object holds a reference to the remainder of the file,
/// because it has at this point only deserialized the table of contents.
/// Reading a stripe will incur further IO and hence can fail.
#[derive(Debug, Clone)]
pub struct ORCFile<F: Read+Seek> {
    schema: Vec<(String, Type)>,
    metadata: messages::Metadata,
    footer: messages::Footer,
    postscript: messages::PostScript,
    file: F
}

impl<F: Read+Seek> ORCFile<F> {
    /// Read a compressed message from the ORC file
    ///
    /// Start and end are relative to the beginning of the file.
    fn read_message<M: Message+Default>(&mut self, start: u64, end: u64) -> OrcResult<M> {
        // Read the compressed data from the file first
        self.file.seek(SeekFrom::Start(start as u64))?;
        let mut comp_buffer_back = vec![0u8; (end-start) as usize];
        self.file.read_exact(&mut comp_buffer_back)?;
        let mut comp_buffer = &comp_buffer_back[..];

        // Start decompressing
        let mut decomp_buffer= vec![];
        while comp_buffer.len() >= 4 {
            let (chunk_len, is_compressed) = match self.postscript.compression() {
                // Messages without compression have no header
                messages::CompressionKind::None => (0, false),
                _ => {
                    let enc = [comp_buffer[0], comp_buffer[1], comp_buffer[2], 0];
                    let enc_len = u32::from_le_bytes(enc);
                    ((enc_len / 2) as usize, (enc_len & 1 == 0))
                }
            };
            match (is_compressed, self.postscript.compression()) {
                (false, messages::CompressionKind::None) => {
                    // Messages without compression have no header
                    decomp_buffer.extend_from_slice(&comp_buffer);
                }
                (false, _) => {
                    // Messages with compression, but where this block is uncompressed still have a header
                    decomp_buffer.extend_from_slice(&comp_buffer[3..chunk_len+3]);
                }
                (true, messages::CompressionKind::Zlib) => {
                    let mut decoder = flate2::read::DeflateDecoder::new(&comp_buffer[3..chunk_len+3]);
                    decoder.read_to_end(&mut decomp_buffer)?;
                }
                (true, messages::CompressionKind::Snappy) => {
                    let mut decoder = snap::read::FrameDecoder::new(&comp_buffer[3..chunk_len+3]);
                    decoder.read_to_end(&mut decomp_buffer)?;
                }
                _ => todo!("Only Zlib and Snappy compression are supported yet.")
                // Lzo = 3,
                // Lz4 = 4,
                // Zstd = 5,
            };
            comp_buffer = &comp_buffer[chunk_len+3..];
        }
        Ok(M::decode(&decomp_buffer[..])?)
    }

    /// Read the table of contents from something readable and seekable and keep the reader
    ///
    /// It isn't finished at this point; more deserializing will be done when you read a stripe.
    /// You can pass a byte vector or a file, but streaming isn't possible because the table of contents
    /// is at the end of the file.
    pub fn from_reader(mut file: F) -> OrcResult<ORCFile<F>> {
        let file_len = file.seek(SeekFrom::End(0))?;
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
            schema: vec![],
            file,
            postscript,
            // read_message wants an ORCFile, but we need read_message to create the members
            // This is fine because these messages have defaults we can overwrite
            metadata: messages::Metadata::default(),
            footer: messages::Footer::default()
        };
        // The file ends with the metadata, footer, postacript, and one last byte for the postscript length
        let postscript_start = file_len - *buffer.last().unwrap() as u64 - 1;
        let footer_start = postscript_start - me.postscript.footer_length();
        let metadata_start = footer_start - me.postscript.metadata_length();
        
        me.footer = me.read_message::<messages::Footer>(footer_start, postscript_start)?;
        me.metadata = me.read_message::<messages::Metadata>(metadata_start, footer_start)?;
        me.schema = me.read_schema(&me.footer.types)?;
        Ok(me)
    }

    /// Unstructured user-defined metadata about this ORC file
    ///
    /// Metadata is usually tiny, and often missing altogether. This may be empty.
    pub fn user_metadata(&self) -> HashMap<String, Vec<u8>> {
        self.footer.metadata.iter()
            .map(|kv| (kv.name().to_string(), kv.value().to_vec()))
            .collect()
    }

    /// Deserialize the schema into a vector of trees
    fn read_schema(&self, raw_types: &[messages::Type]) -> OrcResult<Vec<(String, Type)>> {
        // These are the types not yet claimed as children of another type.
        // At the end of the traversal, these are the roots of the type trees,
        // and are the types of each column, in order
        let mut roots = vec![];

        // Pop consumes the last tree off the roots stack
        // It's helpful because it gives a sensible error instead or panicing if there are no roots
        // It can't own the roots vector because then we couldn't also push to it.
        // All the places we need it will also need to be Boxed to avoid making Type infinitely recursive.
        let pop = |t: &mut Vec<Type>| t.pop()
            .ok_or(OrcError::SchemaError("Missing child type"))
            .map(|x| Box::new(x));

        // The types are given in pre-order,
        // and for lack of imagination we iterate the types in reverse order to create the trees bottom-up.
        // Any unclaimed trees are top-level columns.
        for raw_type in raw_types.iter().rev() {
            // This import helps reduce the noise
            use messages::r#type::Kind;
            let next_type = match raw_type.kind() {
                Kind::Boolean => Type::Boolean,
                Kind::Byte => Type::Byte,
                Kind::Short => Type::Short,
                Kind::Int => Type::Int,
                Kind::Long => Type::Long,
                Kind::Float => Type::Float,
                Kind::Double => Type::Double,
                Kind::String => Type::String,
                Kind::Binary => Type::Binary,
                Kind::Timestamp => Type::Timestamp,
                Kind::List => Type::List(pop(&mut roots)?),
                Kind::Map => {
                    // Undo reverse subtype order
                    let (v, k) = (pop(&mut roots)?, pop(&mut roots)?);
                    Type::Map(k, v)
                },
                Kind::Struct => {
                    if raw_type.field_names.len() != raw_type.subtypes.len() {
                        return Err(OrcError::SchemaError("Field names don't match field types in struct"))
                    }
                    // Keep in mind these subtypes will be in reverse order, note rev() before zip()
                    let subtypes = roots.split_off(roots.len()-raw_type.subtypes.len());
                    // We already own the subtypes but we have to clone the field names
                    Type::Struct(raw_type.field_names.iter().cloned().zip(subtypes.into_iter().rev()).collect())
                },
                Kind::Union => return Err(OrcError::SchemaError("Union types are not yet supported")),
                Kind::Decimal => Type::Decimal{
                    precision: raw_type.precision() as usize,
                    scale: raw_type.scale() as usize
                },
                Kind::Date => Type::Date,
                Kind::Varchar => Type::Varchar(raw_type.maximum_length() as usize),
                Kind::Char => Type::Char(raw_type.maximum_length() as usize),
                Kind::TimestampInstant => Type::TimestampInstant
            };
            roots.push(next_type);
        }
        // I don't see this in the standard but Hive seems to keep everything under one root Struct
        match roots.pop() {
            Some(Type::Struct(x)) => Ok(x),
            None => Err(OrcError::SchemaError("Top level schema is empty")),
            _ => Err(OrcError::SchemaError("Top level schema was not a struct"))
        }
    }

    /// Get a reference to the schema for this file
    ///
    /// Keep in mind that columns can be nested.
    pub fn schema(&self) -> &[(String, Type)] {
        &self.schema
    }

    /// Get information about one stripe in the file
    ///
    /// The stripe requires a mutable borrow because it needs to seek() the underlying reader,
    /// and it would be a mess if they shared cursors.
    /// If you need to use multiple stripes at a time, either open the file multiple times,
    /// with a new OrcFile for each one,
    /// or open an `std::io::Cursor` on an mmap of the file and clone the OrcFile at will.
    /// You can't clone an OrcFile based on std::fs::file but you can clone a mmap based OrcFile.
    pub fn stripe<'t> (&'t mut self, stripe_id: usize) -> OrcResult<Stripe<'t, F>> {
        if stripe_id < self.footer.stripes.len() {
            Stripe::new(stripe_id,self)
        } else {
            Err(OrcError::NoSuchStripe(stripe_id))
        }
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
/// Table Schema
///
/// ORC has a relatively rich type system and supports nested types.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Type {
    Boolean,
    Byte,
    Short,
    Int,
    Long,
    Float,
    Double,
    String,
    Binary,
    Timestamp,
    List(Box<Type>),
    /// Maps store keys and values
    Map(Box<Type>, Box<Type>),
    /// Nested structures are available, with named keys
    Struct(Vec<(String, Type)>),
    /// Hive support for Union is spotty, so avoid Union if possible
    Union(Vec<Type>),
    Decimal{
        precision: usize,
        scale: usize
    },
    Date,
    Varchar(usize),
    Char(usize),
    TimestampInstant
}
/// A handle to one stripe in an ORCFile.
///
/// The stripe requires a mutable borrow because it needs to seek() the underlying reader,
/// and it would be a mess if they shared cursors.
/// If you need to use multiple stripes at a time, either open the file multiple times,
/// with a new OrcFile for each one,
/// or open an `std::io::Cursor` on an mmap of the file and clone the OrcFile at will.
/// You can't clone an OrcFile based on std::fs::file but you can clone a mmap based OrcFile.
pub struct Stripe<'t, F:Read+Seek> {
    file: &'t mut ORCFile<F>,
    id: usize,
    info: messages::StripeInformation,
    footer: messages:: StripeFooter
}
impl<'t, F:Read+Seek> Stripe<'t, F> {
    /// Create a stripe from StripeInformation and a StripeFooter
    ///
    /// The StripeFooter is read from the parent file
    fn new(id: usize, file: &'t mut ORCFile<F>) -> OrcResult<Self> {
        let info = file.footer.stripes[id].clone();
        let footer: messages::StripeFooter = file.read_message(
            info.offset() + info.index_length() + info.data_length(), 
            info.offset() + info.index_length() + info.data_length() + info.footer_length()
        )?;
        Ok(Stripe {id, info, footer, file})
    }

    /// Number of rows in this stripe
    pub fn number_of_rows(&self) -> u64 {
        self.info.number_of_rows()
    }
}
/// Dynamically typed homogenous arrays, with type-specific compression
///
/// 
pub enum BasicStream {
    BooleanRLE,
    ByteRLE,
    IntRLE1,
    IntRLE2,
    UintRLE1,
    UintRLE2,
    Float32,
    Float64,
    Binary
}

impl BasicStream {
    /// Decode a signed varint128 from a slice
    ///
    /// ORC uses the packed-varint128 structure protobuf2/3 uses
    /// but it leaves out the tag prefix so it's not compatible. :/
    /// So we use this shim to decode it instead.
    ///
    /// Returns the number and the remaining bytes.
    pub fn read_i128(buf: &[u8]) -> OrcResult<(i128, &[u8])> {
        let (value, rest) = Self::read_u128(buf)?;
        // Inverse of (n << 1) ^ (n >> 127)
        let zigzag = (value << 127 >> 127) ^ (value >> 1);
        Ok((zigzag as i128, rest))
    }

    /// Decode an unsigned varint128 from a slice
    ///
    /// ORC uses the packed-varint128 structure protobuf2/3 uses
    /// but it leaves out the tag prefix so it's not compatible. :/
    /// So we use this shim to decode it instead.
    ///
    /// Returns the number and the remaining bytes.
    pub fn read_u128(buf: &[u8]) -> OrcResult<(u128, &[u8])> {
        let mut value = 0;
        for i in 0..buf.len() {
            value |= ((buf[i] & 0x7F) << (8*i)) as u128;
            if buf[i] & 0x80 == 0 {
                return Ok((value, &buf[i..]))
            }
        }
        Err(OrcError::TruncatedError)
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
        use super::Type::*;
        let orc_bytes = include_bytes!("sample.orc");
        let orc_toc = super::ORCFile::from_slice(&orc_bytes[..]).unwrap();
        // The metadata is empty
        assert_eq!(orc_toc.user_metadata(), hashmap!{});
        // This is a bit verbose but the point is to stress test the type system
        assert_eq!(orc_toc.schema(), &[
            ("_col0".into(), Boolean),
            ("_col1".into(), Byte),
            ("_col2".into(), Short),
            ("_col3".into(), Int),
            ("_col4".into(), Long),
            ("_col5".into(), Float),
            ("_col6".into(), Double),
            ("_col7".into(), Decimal { precision: 10, scale: 0 }),
            ("_col8".into(), Char(1)),
            ("_col9".into(), Char(3)),
            ("_col10".into(), String),
            ("_col11".into(), Varchar(10)),
            ("_col12".into(), Binary),
            ("_col13".into(), Binary),
            ("_col14".into(), Date),
            ("_col15".into(), Timestamp),
            ("_col16".into(), List(Box::new(Int))),
            ("_col17".into(), List(Box::new(List(Box::new(Int))))),
            ("_col18".into(), Struct(vec![
                ("city".into(), String),
                ("population".into(), Int)
            ])),
            ("_col19".into(), Struct(vec![
                ("city".into(), Struct(vec![
                    ("name".into(), String),
                    ("population".into(), Int)
                ])),
                ("state".into(), String)
            ])),
            ("_col20".into(), List(Box::new(Struct(vec![
                ("city".into(), String),
                ("population".into(), Int)
            ])))),
            ("_col21".into(), Struct(vec![
                ("city".into(), List(Box::new(String))),
                ("population".into(), List(Box::new(Int)))
            ])),
            ("_col22".into(), Map(Box::new(Int), Box::new(String))),
            ("_col23".into(), Map(Box::new(Map(Box::new(Int), Box::new(String))), Box::new(String)))
        ]);
    }

    #[test]
    fn test_stripe() {
        let orc_bytes = include_bytes!("sample.orc");
        let mut orc_toc = super::ORCFile::from_slice(&orc_bytes[..]).unwrap();
        let stripe = orc_toc.stripe(0).unwrap();
        assert_eq!(stripe.number_of_rows(), 3);
    }
}
