#[macro_use] extern crate maplit;
#[macro_use] extern crate hex_literal;
mod errors;
use prost::Message;
use crate::errors::{OrcError, OrcResult};
use std::io::{Read, Seek, SeekFrom, Cursor};
use std::collections::HashMap;

/// Autogenerated messages from the ORC reference implementation's `protobuf` definition
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
    schema: Vec<(String, Schema)>,
    flat_schema: Vec<Schema>,
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
            flat_schema: vec![],
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
        let (schema, flat_schema) = me.read_schema(&me.footer.types)?;
        me.schema = schema;
        me.flat_schema = flat_schema;
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

    /// Deserialize the schema into a tree, and also keep all columns by ID
    ///
    /// Returns a tuple, containing:
    /// * the top level columns in order as a vector with names
    /// * all columns in the order they were stored (preorder by spec)
    fn read_schema(&self, raw_types: &[messages::Type]) -> OrcResult<(Vec<(String, Schema)>, Vec<Schema>)> {
        // These are the types not yet claimed as children of another type.
        // At the end of the traversal, these are the roots of the type trees,
        // and are the types of each column, in order
        let mut roots = vec![];
        let mut all = vec![];

        // Pop consumes the last tree off the roots stack
        // It's helpful because it gives a sensible error instead or panicing if there are no roots
        // It can't own the roots vector because then we couldn't also push to it.
        // All the places we need it will also need to be Boxed to avoid making ColumnSpec infinitely recursive.
        let pop = |t: &mut Vec<Schema>| t.pop()
            .ok_or(OrcError::SchemaError("Missing child type"))
            .map(|x| Box::new(x));

        // The types are given in pre-order,
        // and for lack of imagination we iterate the types in reverse order to create the trees bottom-up.
        // Any unclaimed trees are top-level columns.
        for (id, raw_type) in raw_types.iter().enumerate().rev() {
            // This import helps reduce the noise
            use messages::r#type::Kind;
            let next_type = match raw_type.kind() {
                Kind::Boolean => Schema::Boolean{id},
                Kind::Byte => Schema::Byte{id},
                Kind::Short => Schema::Short{id},
                Kind::Int => Schema::Int{id},
                Kind::Long => Schema::Long{id},
                Kind::Float => Schema::Float{id},
                Kind::Double => Schema::Double{id},
                Kind::String => Schema::String{id},
                Kind::Binary => Schema::Binary{id},
                Kind::Timestamp
                | Kind::TimestampInstant => Schema::Timestamp{id},
                Kind::List => Schema::List{id, inner: pop(&mut roots)?},
                Kind::Map => {
                    // Undo reverse subtype order
                    let (value, key) = (pop(&mut roots)?, pop(&mut roots)?);
                    Schema::Map{id, key, value}
                },
                Kind::Struct => {
                    if raw_type.field_names.len() != raw_type.subtypes.len() {
                        return Err(OrcError::SchemaError("Field names don't match field types in struct"))
                    }
                    // Keep in mind these subtypes will be in reverse order, note rev() before zip()
                    let subtypes = roots.split_off(roots.len()-raw_type.subtypes.len());
                    // We already own the subtypes but we have to clone the field names
                    let fields = raw_type.field_names.iter().cloned().zip(subtypes.into_iter().rev()).collect();
                    Schema::Struct{id, fields}
                },
                Kind::Union => return Err(OrcError::SchemaError("Union types are not yet supported")),
                Kind::Decimal => Schema::Decimal{
                    id,
                    precision: raw_type.precision() as usize,
                    scale: raw_type.scale() as usize
                },
                Kind::Date => Schema::Date{id},
                Kind::Varchar => Schema::Varchar{id, length: raw_type.maximum_length() as usize},
                Kind::Char => Schema::Char{id, length: raw_type.maximum_length() as usize},
            };
            roots.push(next_type.clone());
            all.push(next_type);
        }
        // The flat schema is backward at this point
        all.reverse();
        // I don't see this in the standard but Hive seems to keep everything under one root Struct
        match roots.pop() {
            Some(Schema::Struct{fields, ..}) => Ok((fields, all)),
            None => Err(OrcError::SchemaError("Top level schema is empty")),
            _ => Err(OrcError::SchemaError("Top level schema was not a struct"))
        }
    }

    /// Get a reference to the schema for this file
    ///
    /// Keep in mind that columns can be nested.
    pub fn schema(&self) -> &[(String, Schema)] {
        &self.schema
    }

    /// Get a reference to all columns and their descendants
    ///
    /// These are still trees, but in this case all subtrees are listed in the top level vector.
    /// Note that for this reason field names are not given. Not all types have field names.
    pub fn flat_schema(&self) -> &[Schema] {
        &self.flat_schema
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
pub enum Schema {
    Boolean {id: usize},
    Byte {id: usize},
    Short {id: usize},
    Int {id: usize},
    Long {id: usize},
    Float {id: usize},
    Double {id: usize},
    String {id: usize},
    Binary {id: usize},
    Timestamp {id: usize},
    // TODO: Why is there a second timestamp type named TimestampInstant?
    List {id: usize, inner: Box<Schema> },
    /// Maps store keys and values
    Map {id: usize, key: Box<Schema>, value: Box<Schema> },
    /// Nested structures are available, with named keys
    Struct {id: usize, fields: Vec<(String, Schema)> },
    /// Hive support for Union is spotty, and we don't support it.
    Union{id: usize},
    Decimal{
        id: usize,
        precision: usize,
        scale: usize
    },
    Date {id: usize},
    Varchar {id: usize, length: usize},
    Char {id: usize, length: usize}
}
impl Schema {
    /// Get the column ID of any type
    pub fn id(&self) -> usize {
        match self {
            Self::Boolean{id}
            | Self::Byte{id}
            | Self::Short{id}
            | Self::Int{id}
            | Self::Long{id}
            | Self::Float{id}
            | Self::Double{id}
            | Self::String{id}
            | Self::Binary{id}
            | Self::Timestamp{id}
            | Self::List{id, ..}
            | Self::Map{id, ..}
            | Self::Struct{id, ..}
            | Self::Union{id}
            | Self::Decimal{id, ..}
            | Self::Date{id}
            | Self::Varchar{id, ..}
            | Self::Char{id, ..} => *id
        }
    }
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
    info: messages::StripeInformation,
    footer: messages::StripeFooter
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
        Ok(Stripe {info, footer, file})
    }

    /// Number of rows in this stripe
    pub fn number_of_rows(&self) -> u64 {
        self.info.number_of_rows()
    }

    /// Return the columns available in this stripe
    pub fn columns(&self) -> OrcResult<Vec<ColumnRef>> {
        let flat_schema = self.file.flat_schema();
        if self.footer.columns.len() != flat_schema.len() {
            return Err(OrcError::SchemaError("Stripe column definitions don't match schema"))
        }
        let mut cols = vec![];
        for id in 0..flat_schema.len() {
            cols.push(ColumnRef::new(
                &flat_schema[id], 
                &self.footer.columns[id], 
                &self.footer.streams[..])?
            );
        }
        Ok(cols)
    }

}
#[derive(Debug, Eq, PartialEq)]
pub struct ColumnRef {
    schema: Schema,
    present: Option<Encoded>,
    content: ColumnSpec
}
impl ColumnRef {
    pub fn new(
        schema: &Schema,
        enc: &messages::ColumnEncoding,
        streams: &[messages::Stream]
    ) -> OrcResult<ColumnRef> {
        use messages::column_encoding::Kind as Ckind;
        use messages::stream::Kind as Skind;
        let range_by_kind = |sk: Skind| streams.iter()
            .scan(0u64, |cur, st| {
                // The offset of each stream is the cumulative sum of the lengths
                let start = *cur;
                *cur += st.length();
                Some((start..*cur, st))
            })
            .find(|(_, stream)| stream.column() as usize == schema.id() && stream.kind() == sk)
            .map(|(rng, _) | rng)
            .ok_or(OrcError::EncodingError(format!("Column {} missing {:?} stream", schema.id(), sk)));
        // Most colspecs need these streams, so tee them up to save writing
        let data = range_by_kind(Skind::Data);
        let len = range_by_kind(Skind::Length);

        // These integer encodings are mostly orthogonal to the types
        let int_enc = |r| match enc.kind() {
            Ckind::Direct | Ckind::Dictionary => Encoded(Codec::IntRLE1, r),
            Ckind::DirectV2 | Ckind::DictionaryV2 => Encoded(Codec::IntRLE2, r)
        };
        let uint_enc = |r| match enc.kind() {
            Ckind::Direct | Ckind::Dictionary => Encoded(Codec::UintRLE1, r),
            Ckind::DirectV2 | Ckind::DictionaryV2 => Encoded(Codec::UintRLE2, r)
        };
        let content = match schema {
            Schema::Boolean{..} => ColumnSpec::Boolean{data: Encoded(Codec::BooleanRLE, data?)},
            Schema::Byte{..}   => ColumnSpec::Byte{data: Encoded(Codec::ByteRLE, data?)},
            Schema::Short{..}
            | Schema::Int{..}
            | Schema::Long{..}
            | Schema::Date{..} => ColumnSpec::Int{data: int_enc(data?)},
            Schema::Float{..}
            | Schema::Double{..} => ColumnSpec::Float{data: Encoded(Codec::FloatEnc, data?)},
            Schema::Decimal{..} => ColumnSpec::Decimal{
                data: int_enc(data?),
                scale: int_enc(range_by_kind(Skind::Secondary)?)
            },
            Schema::String{..}
            | Schema::Varchar{..}
            | Schema::Char{..}
            | Schema::Binary{..} => ColumnSpec::Blob{
                data: Encoded(Codec::BinaryEnc, data?),
                length: uint_enc(len?)
            },
            Schema::Map{..}
            | Schema::List{..} => ColumnSpec::Container{length: uint_enc(len?)},
            Schema::Struct{..} => ColumnSpec::Struct(),
            Schema::Timestamp{..} => ColumnSpec::Timestamp{
                seconds: int_enc(data?),
                nanos: match enc.kind() {
                    Ckind::Direct => Encoded(Codec::NanosEnc1, range_by_kind(Skind::Secondary)?),
                    Ckind::DirectV2 => Encoded(Codec::NanosEnc2, range_by_kind(Skind::Secondary)?),
                    _ => return Err(OrcError::EncodingError(format!("Timestamp doesn't support dictionary encoding")))
                }
            },
            Schema::Union{..} => return Err(OrcError::SchemaError("Union types are not supported")),
        };
        Ok(ColumnRef {
            schema: schema.clone(),
            present: range_by_kind(Skind::Present)
                .map(|r| Encoded(Codec::BooleanRLE, r))
                .ok(),
            content
        })
    }
}

/// Joins multiple streams to create different composite types
///
/// This is where variable length types, like strings, are handled.
#[derive(Debug, Eq, PartialEq)]
pub enum ColumnSpec {
    /// Boolean RLE
    Boolean {data: Encoded},
    /// RLE encoding for 8-bit integers and UNIONS. Unions aren't supported.
    Byte {data: Encoded},
    /// `DIRECT` encoding, for all int sizes, booleans, bytes, dates.
    /// For dates, the data is the days since 1970.  
    Int {data: Encoded},
    /// `DIRECT` encoding for floats
    Float {data: Encoded},
    /// `DIRECT` encoding for chars, strings, varchars, and blobs
    Blob {data: Encoded, length: Encoded},
    /// For maps and lists, the data is the length.
    Container {length: Encoded},
    /// `DICTIONARY` encoding, only for chars, strings and varshars (not blobs)
    Dict {data: Encoded, length: Encoded, dict: Encoded},
    /// Decimals, which store i128's with an associated scale.
    Decimal {data: Encoded, scale: Encoded },
    /// Timestamps, which store time since 1970, plus a second stream with nanoseconds
    Timestamp {seconds: Encoded, nanos: Encoded },
    /// Structs only store if they are present. The rest is in other columns.
    Struct (),
    // Unions are not supported.
}

#[derive(Debug, Eq, PartialEq)]
pub struct Encoded(Codec, std::ops::Range<u64>);

/// Basic over the wire types, all primitives with a type-specific compression method
#[derive(Debug, Eq, PartialEq)]
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

/// Byte level RLE, encoding up to 128 byte literals and up to 130 byte runs
/// It works pretty much like it sounds, a flag followed by a string (for literals),
/// or by a single byte (for runs)
#[derive(Debug)]
pub(crate) struct ByteRLEDecoder<'t> {
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
impl<'t> Iterator for ByteRLEDecoder<'t> {
    type Item = u8;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            // Nothing left (even if it's the middle of a run or literal)
            None
        } else if self.run > 0 {
            // Repeat the last byte
            self.run -= 1;
            Some(self.run_item)
        } else if self.literal > 0 {
            // Consume a byte
            self.literal -= 1;
            let first = self.buf[0];
            self.buf = &self.buf[1..];
            Some(first)
        } else if self.buf.len() < 2 {
            // All other branches need two bytes
            None
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
            self.next()
        }
    }
}

/// Boolean encoder based on byte-level RLE
#[derive(Debug)]
pub(crate) struct BooleanRLEDecoder<'t> {
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
impl<'t> Iterator for BooleanRLEDecoder<'t> {
    type Item = bool;
    fn next(&mut self) -> Option<Self::Item> {
        if self.bit == 0 {
            // Try to advance to the next byte
            match self.buf.next() {
                None => return None,
                Some(byte ) => {
                    self.byte = byte;
                    self.bit = 8;
                }
            }
        }
        self.bit -= 1;
        Some((self.byte & (1 << self.bit)) != 0)
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
        use super::Schema as Sch;
        let orc_bytes = include_bytes!("sample.orc");
        let orc_toc = super::ORCFile::from_slice(&orc_bytes[..]).unwrap();
        // The metadata is empty
        assert_eq!(orc_toc.user_metadata(), hashmap!{});
        // This is a bit verbose but the point is to stress test the type system
        assert_eq!(orc_toc.schema(), &[
            ("_col0".into(), Sch::Boolean { id: 1 }),
            ("_col1".into(), Sch::Byte { id: 2 }),
            ("_col2".into(), Sch::Short { id: 3 }),
            ("_col3".into(), Sch::Int { id: 4 }),
            ("_col4".into(), Sch::Long { id: 5 }),
            ("_col5".into(), Sch::Float { id: 6 }),
            ("_col6".into(), Sch::Double { id: 7 }),
            ("_col7".into(), Sch::Decimal { id: 8, precision: 10, scale: 0 }),
            ("_col8".into(), Sch::Char { id: 9, length: 1 }),
            ("_col9".into(), Sch::Char { id: 10, length: 3 }),
            ("_col10".into(), Sch::String { id: 11 }),
            ("_col11".into(), Sch::Varchar { id: 12, length: 10 }),
            ("_col12".into(), Sch::Binary { id: 13 }),
            ("_col13".into(), Sch::Binary { id: 14 }),
            ("_col14".into(), Sch::Date { id: 15 }),
            ("_col15".into(), Sch::Timestamp { id: 16 }),
            ("_col16".into(), Sch::List { id: 17, inner: Box::new(Sch::Int { id: 18 }) }),
            ("_col17".into(), Sch::List { id: 19, inner: Box::new(Sch::List { id: 20, inner: Box::new(Sch::Int { id: 21 }) }) }),
            ("_col18".into(), Sch::Struct { id: 22, fields: vec![
                ("city".into(), Sch::String { id: 23 }),
                ("population".into(), Sch::Int { id: 24 })
            ] }),
            ("_col19".into(), Sch::Struct { id: 25, fields: vec![
                ("city".into(), Sch::Struct { id: 26, fields: vec![
                    ("name".into(), Sch::String { id: 27 }),
                    ("population".into(), Sch::Int { id: 28 })
                ] }),
                ("state".into(), Sch::String { id: 29 })
            ] }),
            ("_col20".into(), Sch::List { id: 30, inner: Box::new(Sch::Struct { id: 31, fields: vec![
                ("city".into(), Sch::String { id: 32 }),
                ("population".into(), Sch::Int { id: 33 })
            ] }) }),
            ("_col21".into(), Sch::Struct { id: 34, fields: vec![
                ("city".into(), Sch::List { id: 35, inner: Box::new(Sch::String { id: 36 }) }),
                ("population".into(), Sch::List { id: 37, inner: Box::new(Sch::Int { id: 38 }) })
            ] }),
            ("_col22".into(), Sch::Map {
                id: 39,
                key: Box::new(Sch::Int { id: 41 }),
                value: Box::new(Sch::String { id: 40 })
            }),
            ("_col23".into(), Sch::Map {
                id: 42,
                key: Box::new(Sch::Map {
                    id: 44,
                    key: Box::new(Sch::Int { id: 46 }),
                    value: Box::new(Sch::String { id: 45 })
                }),
                value: Box::new(Sch::String { id: 43 })
            })
        ]);
    }

    #[test]
    fn test_stripe() {
        use super::{ColumnRef, Encoded, Codec, ColumnSpec as TS};
        let orc_bytes = include_bytes!("sample.orc");
        let mut orc_toc = super::ORCFile::from_slice(&orc_bytes[..]).unwrap();
        let schema = orc_toc.flat_schema().to_vec();
        let stripe = orc_toc.stripe(0).unwrap();
        assert_eq!(stripe.number_of_rows(), 3);
        assert_eq!(stripe.columns().unwrap(), vec![
            ColumnRef { schema: schema[0].clone(), present: None, content: TS::Struct() },
            ColumnRef { schema: schema[1].clone(), present: Some(Encoded(Codec::BooleanRLE, 1149..1154)), content: TS::Boolean { data: Encoded(Codec::BooleanRLE, 1154..1159) } },
            ColumnRef { schema: schema[2].clone(), present: Some(Encoded(Codec::BooleanRLE, 1159..1164)), content: TS::Byte { data: Encoded(Codec::ByteRLE, 1164..1170) } },
            ColumnRef { schema: schema[3].clone(), present: Some(Encoded(Codec::BooleanRLE, 1170..1175)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1175..1182) } },
            ColumnRef { schema: schema[4].clone(), present: Some(Encoded(Codec::BooleanRLE, 1182..1187)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1187..1194) } },
            ColumnRef { schema: schema[5].clone(), present: Some(Encoded(Codec::BooleanRLE, 1194..1199)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1199..1206) } },
            ColumnRef { schema: schema[6].clone(), present: Some(Encoded(Codec::BooleanRLE, 1206..1211)), content: TS::Float { data: Encoded(Codec::FloatEnc, 1211..1222) } },
            ColumnRef { schema: schema[7].clone(), present: Some(Encoded(Codec::BooleanRLE, 1222..1227)), content: TS::Float { data: Encoded(Codec::FloatEnc, 1227..1242) } },
            ColumnRef { schema: schema[8].clone(), present: Some(Encoded(Codec::BooleanRLE, 1242..1247)), content: TS::Decimal { data: Encoded(Codec::IntRLE2, 1247..1252), scale: Encoded(Codec::IntRLE2, 1252..1258) } },
            ColumnRef { schema: schema[9].clone(), present: Some(Encoded(Codec::BooleanRLE, 1258..1263)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1263..1268), length: Encoded(Codec::UintRLE2, 1268..1274) } },
            ColumnRef { schema: schema[10].clone(), present: Some(Encoded(Codec::BooleanRLE, 1274..1279)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1279..1288), length: Encoded(Codec::UintRLE2, 1288..1294) } },
            ColumnRef { schema: schema[11].clone(), present: Some(Encoded(Codec::BooleanRLE, 1294..1299)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1299..1305), length: Encoded(Codec::UintRLE2, 1305..1311) } },
            ColumnRef { schema: schema[12].clone(), present: Some(Encoded(Codec::BooleanRLE, 1311..1316)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1316..1322), length: Encoded(Codec::UintRLE2, 1322..1328) } },
            ColumnRef { schema: schema[13].clone(), present: Some(Encoded(Codec::BooleanRLE, 1328..1333)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1333..1339), length: Encoded(Codec::UintRLE2, 1339..1345) } },
            ColumnRef { schema: schema[14].clone(), present: Some(Encoded(Codec::BooleanRLE, 1345..1350)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1350..1358), length: Encoded(Codec::UintRLE2, 1358..1364) } },
            ColumnRef { schema: schema[15].clone(), present: Some(Encoded(Codec::BooleanRLE, 1364..1369)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1369..1380) } },
            ColumnRef { schema: schema[16].clone(), present: Some(Encoded(Codec::BooleanRLE, 1380..1385)), content: TS::Timestamp { seconds: Encoded(Codec::IntRLE2, 1385..1398), nanos: Encoded(Codec::NanosEnc2, 1398..1407) } },
            ColumnRef { schema: schema[17].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1407..1412) } },
            ColumnRef { schema: schema[18].clone(), present: Some(Encoded(Codec::BooleanRLE, 1412..1418)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1418..1428) } },
            ColumnRef { schema: schema[19].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1428..1433) } },
            ColumnRef { schema: schema[20].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1433..1438) } },
            ColumnRef { schema: schema[21].clone(), present: Some(Encoded(Codec::BooleanRLE, 1438..1444)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1444..1454) } },
            ColumnRef { schema: schema[22].clone(), present: None, content: TS::Struct() },
            ColumnRef { schema: schema[23].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1454..1459), length: Encoded(Codec::UintRLE2, 1459..1465) } },
            ColumnRef { schema: schema[24].clone(), present: Some(Encoded(Codec::BooleanRLE, 1479..1484)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1484..1491) } },
            ColumnRef { schema: schema[25].clone(), present: None, content: TS::Struct() },
            ColumnRef { schema: schema[26].clone(), present: None, content: TS::Struct() },
            ColumnRef { schema: schema[27].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1491..1496), length: Encoded(Codec::UintRLE2, 1496..1502) } },
            ColumnRef { schema: schema[28].clone(), present: Some(Encoded(Codec::BooleanRLE, 1516..1521)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1521..1528) } },
            ColumnRef { schema: schema[29].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1528..1533), length: Encoded(Codec::UintRLE2, 1533..1539) } },
            ColumnRef { schema: schema[30].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1544..1549) } },
            ColumnRef { schema: schema[31].clone(), present: None, content: TS::Struct() },
            ColumnRef { schema: schema[32].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1549..1555), length: Encoded(Codec::UintRLE2, 1555..1561) } },
            ColumnRef { schema: schema[33].clone(), present: Some(Encoded(Codec::BooleanRLE, 1582..1587)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1587..1600) } },
            ColumnRef { schema: schema[34].clone(), present: None, content: TS::Struct() },
            ColumnRef { schema: schema[35].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1600..1605) } },
            ColumnRef { schema: schema[36].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1605..1611), length: Encoded(Codec::UintRLE2, 1611..1617) } },
            ColumnRef { schema: schema[37].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1638..1643) } },
            ColumnRef { schema: schema[38].clone(), present: Some(Encoded(Codec::BooleanRLE, 1643..1648)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1648..1661) } },
            ColumnRef { schema: schema[39].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1661..1666) } },
            ColumnRef { schema: schema[40].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1666..1672), length: Encoded(Codec::UintRLE2, 1672..1678) } },
            ColumnRef { schema: schema[41].clone(), present: Some(Encoded(Codec::BooleanRLE, 1690..1695)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1695..1708) } },
            ColumnRef { schema: schema[42].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1708..1713) } },
            ColumnRef { schema: schema[43].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1713..1718), length: Encoded(Codec::UintRLE2, 1718..1724) } },
            ColumnRef { schema: schema[44].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1731..1736) } },
            ColumnRef { schema: schema[45].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1736..1741), length: Encoded(Codec::UintRLE2, 1741..1747) } },
            ColumnRef { schema: schema[46].clone(), present: Some(Encoded(Codec::BooleanRLE, 1755..1760)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1760..1769) } }
        ]);
    }

    #[test]
    fn test_byte_rle_decoder() {
        let encoded_test = hex!("FC DEAD BEEF 03 00 FC CAFE BABE");
        let dec: Vec<u8> = super::ByteRLEDecoder::from(&encoded_test[..]).collect();
        assert_eq!(dec, hex!("DEAD BEEF 00 00 00 00 00 00 CAFE BABE"));
    }

    #[test]
    fn test_bool_rle_decoder() {
        let encoded_test = hex!("FF 80");
        let dec: Vec<bool> = super::BooleanRLEDecoder::from(&encoded_test[..]).collect();
        assert_eq!(dec, &[true, false, false, false, false, false, false, false]);
    }
}
