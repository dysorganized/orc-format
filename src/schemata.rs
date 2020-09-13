use std::collections::HashMap;
use std::io::{Read, Seek};
use crate::codecs::{Codec, PrimitiveBuffer};
use crate::toc::{ORCFile, Stripe};
use crate::messages;
use crate::errors::*;

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

#[derive(Debug, PartialEq)]
pub struct ColumnRef<'t> {
    pub(crate) stripe: &'t Stripe,
    pub(crate) schema: Schema,
    pub(crate) present: Option<Encoded>,
    pub(crate) content: ColumnSpec
}
impl<'t> ColumnRef<'t> {
    pub fn new(
        stripe: &'t Stripe,
        schema: &Schema,
        enc: &messages::ColumnEncoding,
        streams: &HashMap<(u32, messages::stream::Kind), std::ops::Range<u64>>
    ) -> OrcResult<ColumnRef<'t>> {
        use messages::column_encoding::Kind as Ckind;
        use messages::stream::Kind as Skind;
        let range_by_kind = |sk: Skind| streams
            .get(&(schema.id() as u32, sk))
            .cloned()
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
            stripe,
            schema: schema.clone(),
            present: range_by_kind(Skind::Present)
                .map(|r| Encoded(Codec::BooleanRLE, r))
                .ok(),
            content
        })
    }

    /// Read the column from the same open ORC file.
    ///
    /// The read will only succeed for the same ORC file.
    pub fn read_from<F: Read+Seek>(&self, file: &mut ORCFile<F>) -> OrcResult<()> {
        if let ColumnSpec::Int{data} = &self.content {
            // All the streams, even content streams, start their indices at the index start, not content start.
            let enc = data;
            let start = self.stripe.info.offset() + data.1.start;
            let buf = file.read_compressed(start..start + enc.1.end - enc.1.start)?;
            //file.file.seek(SeekFrom::Start(start))?;
            //let mut buf = vec![0; data.1.end as usize - data.1.start as usize];
            //file.file.read_exact(&mut buf[..])?; 
            let prim_stream = data.decode(&buf)?;
            match prim_stream {
                PrimitiveBuffer::Int128(idec) => println!("Got ints {:?}", idec),
                x => println!("nope for {:?}", x)
            }
        }
        Ok(())
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
    /// `DICTIONARY` encoding, only for chars, strings and varchars (not blobs)
    Dict {data: Encoded, length: Encoded, dict: Encoded},
    /// Decimals, which store i128's with an associated scale.
    Decimal {data: Encoded, scale: Encoded },
    /// Timestamps, which store time since 1970, plus a second stream with nanoseconds
    Timestamp {seconds: Encoded, nanos: Encoded },
    /// Structs only store if they are present. The rest is in other columns.
    Struct (),
    // Unions are not supported.
}

/// Part of a stripe with a known encoding
///
/// This doesn't have a reference to the buffer because it may not have been read yet.
/// (the offsets are available in the stripe footer).
/// Once you have read the associated stripe, you can read it with the decode method.
#[derive(Debug, Eq, PartialEq)]
pub struct Encoded(pub Codec, pub std::ops::Range<u64>);
impl Encoded {
    /// Decode part of a buffer using this codec and subslice.
    pub fn decode<'t>(&self, buf: &'t [u8]) -> OrcResult<PrimitiveBuffer> {
        self.0.decode(buf)
    }
}
