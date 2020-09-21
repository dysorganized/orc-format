use crate::codecs;
use crate::errors::*;
use crate::messages;
use crate::toc::{ORCFile, Stripe};
use bytes::Bytes;
use serde_json as json;
use std::io::{BufRead, Read, Seek};
use std::{collections::HashMap, convert::TryFrom, convert::TryInto};

/// Table Schema
///
/// ORC has a relatively rich type system and supports nested types.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Schema {
    Boolean {
        id: usize,
    },
    Byte {
        id: usize,
    },
    Short {
        id: usize,
    },
    Int {
        id: usize,
    },
    Long {
        id: usize,
    },
    Float {
        id: usize,
    },
    Double {
        id: usize,
    },
    String {
        id: usize,
    },
    Binary {
        id: usize,
    },
    Timestamp {
        id: usize,
    },
    // TODO: Why is there a second timestamp type named TimestampInstant?
    List {
        id: usize,
        inner: Box<Schema>,
    },
    /// Maps store keys and values
    Map {
        id: usize,
        key: Box<Schema>,
        value: Box<Schema>,
    },
    /// Nested structures are available, with named keys
    Struct {
        id: usize,
        fields: Vec<(String, Schema)>,
    },
    /// Hive support for Union is spotty, and we don't support it.
    Union {
        id: usize,
    },
    Decimal {
        id: usize,
        precision: usize,
        scale: usize,
    },
    Date {
        id: usize,
    },
    Varchar {
        id: usize,
        length: usize,
    },
    Char {
        id: usize,
        length: usize,
    },
}
impl Schema {
    /// Get the column ID of any type
    pub fn id(&self) -> usize {
        match self {
            Self::Boolean { id }
            | Self::Byte { id }
            | Self::Short { id }
            | Self::Int { id }
            | Self::Long { id }
            | Self::Float { id }
            | Self::Double { id }
            | Self::String { id }
            | Self::Binary { id }
            | Self::Timestamp { id }
            | Self::List { id, .. }
            | Self::Map { id, .. }
            | Self::Struct { id, .. }
            | Self::Union { id }
            | Self::Decimal { id, .. }
            | Self::Date { id }
            | Self::Varchar { id, .. }
            | Self::Char { id, .. } => *id,
        }
    }
}

/// A vectors of nullable elements of various types.
///
/// Null elements are represented with a separate stream of boolean values.
/// But unlike a mask, where the "present" stream is false, the corresponding element is omitted.
/// Hence sparse vectors may have significant space savings.  
/// Vectors with no nulls have the "present" stream omitted instead, also saving modest space.
/// The downside is that most interesting operations require an additional copy to impute the nulls.
/// Similarly, the overhead of `Option<X>` may outweigh the target computation.
#[derive(Debug, PartialEq)]
pub enum Column {
    Boolean {
        present: Option<Vec<bool>>,
        data: Vec<bool>,
    },
    Byte {
        present: Option<Vec<bool>>,
        data: Vec<u8>,
    },
    Int {
        present: Option<Vec<bool>>,
        data: Vec<i128>,
    },
    Float {
        present: Option<Vec<bool>>,
        data: Vec<f32>,
    },
    Double {
        present: Option<Vec<bool>>,
        data: Vec<f64>,
    },

    /// A vector of strings, stored as a concatenated 1D array, and an array of lengths  
    Blob {
        present: Option<Vec<bool>>,
        data: Vec<u8>,
        length: Vec<u128>,
        utf8: bool,
    },
    /// A vector of blobs from a fixed set, stored as three arrays
    /// * A dictionary, stored like the usual blob vector:
    ///   * The set of distinct blobs, concatenated
    ///   * The lengths of each distinct blob
    /// * Encoded values:
    ///   * For each cell, the index of the corresponding blob in the dictionary
    BlobDict {
        present: Option<Vec<bool>>,
        data: Vec<u128>,
        dictionary_data: Vec<u8>,
        length: Vec<u128>,
        utf8: bool,
    },

    /// A list, like a blob, is a length vector and a concatenated array of elements
    List {
        present: Option<Vec<bool>>,
        length: Vec<u128>,
        elements: Box<Column>,
    },
    /// A map is like two aligned List columns
    Map {
        present: Option<Vec<bool>>,
        length: Vec<u128>,
        keys: Box<Column>,
        values: Box<Column>,
    },

    /// Decimals, which store i128's with an associated scale.
    Decimal {
        present: Option<Vec<bool>>,
        data: Vec<i128>,
        scale: Vec<i128>,
    },
    /// Timestamps, which store time since 1970, plus a second stream with nanoseconds
    Timestamp {
        present: Option<Vec<bool>>,
        seconds: Vec<i128>,
        nanos: Vec<u128>,
    },
    /// Structs only store if they are present. The rest is in other columns.
    Struct {
        present: Option<Vec<bool>>,
    },
    // Unions are not supported.
    Unsupported(&'static str),
}

impl Column {
    pub fn new<F: Read + Seek>(
        stripe: &Stripe,
        schema: &Schema,
        enc: &messages::ColumnEncoding,
        streams: &HashMap<(u32, messages::stream::Kind), std::ops::Range<u64>>,
        orc: &mut ORCFile<F>,
    ) -> OrcResult<Column> {
        use messages::column_encoding::Kind as Ckind;
        use messages::stream::Kind as Skind;
        let mut slice_by_kind = |sk: Skind| {
            streams
                .get(&(schema.id() as u32, sk))
                .cloned()
                .ok_or(OrcError::EncodingError(format!(
                    "Column {} missing {:?} stream",
                    schema.id(),
                    sk
                )))
                .and_then(|rng| {
                    orc.read_compressed(
                        stripe.info.offset() + rng.start..stripe.info.offset() + rng.end,
                    )
                })
        };
        // Most colspecs need these streams, so tee them up to save writing
        let data = slice_by_kind(Skind::Data);
        let len = slice_by_kind(Skind::Length);
        let present = slice_by_kind(Skind::Present)
            .and_then(|r| codecs::BooleanRLEDecoder::from(&r[..]).collect::<OrcResult<Vec<bool>>>())
            .ok();

        // These integer encodings are mostly orthogonal to the types
        let int_enc = |rs: Bytes| match enc.kind() {
            Ckind::Direct | Ckind::Dictionary => {
                codecs::RLE1::<i128>::from(&rs[..]).collect::<OrcResult<Vec<i128>>>()
            }
            Ckind::DirectV2 | Ckind::DictionaryV2 => {
                codecs::RLE2::<i128>::from(&rs[..]).collect::<OrcResult<Vec<i128>>>()
            }
        };
        let uint_enc = |rs: Bytes| match enc.kind() {
            Ckind::Direct | Ckind::Dictionary => {
                codecs::RLE1::<u128>::from(&rs[..]).collect::<OrcResult<Vec<u128>>>()
            }
            Ckind::DirectV2 | Ckind::DictionaryV2 => {
                codecs::RLE2::<u128>::from(&rs[..]).collect::<OrcResult<Vec<u128>>>()
            }
        };
        let content = match schema {
            // Boolean has it's own decoder
            Schema::Boolean { .. } => Column::Boolean {
                present,
                data: codecs::BooleanRLEDecoder::from(&data?[..])
                    // Booleans need to be trimmed because they round up to the nearest 8
                    .take(stripe.info.number_of_rows() as usize)
                    .collect::<OrcResult<_>>()?,
            },

            // Byte has it's own decoder
            Schema::Byte { .. } => Column::Byte {
                present,
                data: codecs::ByteRLEDecoder::from(&data?[..]).collect::<OrcResult<_>>()?,
            },

            // All the integers work the same
            // We promote them all to i128 for simplicity as a POC
            // TODO: Specialize the integers
            Schema::Short { .. }
            | Schema::Int { .. }
            | Schema::Long { .. }
            | Schema::Date { .. } => Column::Int {
                present,
                data: int_enc(data?)?,
            },

            // Float32, which has it's own decoder (the bit length is part of the encoder)
            Schema::Float { .. } => Column::Float {
                present,
                data: codecs::FloatDecoder::from(&data?[..]).collect::<OrcResult<_>>()?,
            },

            // Float64 has it's own decoder too
            Schema::Double { .. } => Column::Double {
                present,
                data: codecs::DoubleDecoder::from(&data?[..]).collect::<OrcResult<_>>()?,
            },

            // Decimals are huge ints with a scale, really just a knockoff IEEE754 again
            Schema::Decimal { .. } => Column::Decimal {
                present,
                data: int_enc(data?)?,
                scale: int_enc(slice_by_kind(Skind::Secondary)?)?,
            },

            // There are eight flavors of blob columns made from three binary choices:
            // * With and without encoding (with=string, without=blob)
            // * With RLE1 and RLE2 (int_enc handles that for us)
            // * Stored or Dictionary-compresses
            Schema::Binary { .. }
            | Schema::String { .. }
            | Schema::Varchar { .. }
            | Schema::Char { .. } => {
                // We don't do any decoding here but just wait until someone uses it.
                // So all we need to know is whether it will need to be done later.
                let utf8 = if let Schema::Binary { .. } = schema {
                    false
                } else {
                    true
                };
                // Similarly, the dictionary decoding is not done until later
                // But it will require two different enums
                match enc.kind() {
                    Ckind::Direct | Ckind::DirectV2 => Column::Blob {
                        present,
                        data: data?.to_vec(),
                        length: uint_enc(len?)?,
                        utf8,
                    },
                    Ckind::Dictionary | Ckind::DictionaryV2 => Column::BlobDict {
                        present,
                        data: uint_enc(data?)?,
                        dictionary_data: slice_by_kind(Skind::DictionaryData)?.to_vec(),
                        length: uint_enc(len?)?,
                        utf8,
                    },
                }
            }
            Schema::Map { .. } => Column::Unsupported("Map columns are not supported yet."),
            Schema::List { inner, .. } => Column::List {
                present,
                length: uint_enc(len?)?,
                elements: Box::new(Column::new(stripe, inner, enc, streams, orc)?),
            },
            Schema::Struct { .. } => Column::Struct { present },
            Schema::Timestamp { .. } => {
                // It could look something like this:
                // ColumnContent::Timestamp{
                //     seconds: int_enc(data?),
                //     nanos: match enc.kind() {
                //         Ckind::Direct => Codec::NanosEnc1.decode(range_by_kind(Skind::Secondary)?),
                //         Ckind::DirectV2 => Codec::NanosEnc2.decode(range_by_kind(Skind::Secondary)?),
                //         _ => return Err(OrcError::EncodingError(format!("Timestamp doesn't support dictionary encoding")))
                //     }
                // }
                Column::Unsupported("Timestamp decoding is not supported yet for Column decoding")
            }
            Schema::Union { .. } => {
                Column::Unsupported("Union columns are not well defined or supported")
            }
        };
        Ok(content)
    }

    /// Get the name of the variant as a str
    fn variant_name(&self) -> &'static str {
        match self {
            Self::Boolean { .. } => "Boolean",
            Self::Byte { .. } => "Byte",
            Self::Int { .. } => "Int",
            Self::Float { .. } => "Float",
            Self::Double { .. } => "Double",
            Self::Blob { .. } => "Blob",
            Self::BlobDict { .. } => "BlobDict",
            Self::Map { .. } => "Map",
            Self::List { .. } => "List",
            Self::Decimal { .. } => "Decimal",
            Self::Timestamp { .. } => "Timestamp",
            Self::Struct { .. } => "Struct",
            Self::Unsupported(..) => "Unsupported",
        }
    }

    /// A stream indicating whether the row is non-null
    ///
    /// If it's empty (e.g. None, not length 0) then all rows are present.
    /// Keep in mind it's frequently the case all rows are present so expect None
    pub fn present(&self) -> &Option<Vec<bool>> {
        match self {
            Self::Boolean { present, .. }
            | Self::Byte { present, .. }
            | Self::Int { present, .. }
            | Self::Float { present, .. }
            | Self::Double { present, .. }
            | Self::Blob { present, .. }
            | Self::BlobDict { present, .. }
            | Self::Map { present, .. }
            | Self::List { present, .. }
            | Self::Decimal { present, .. }
            | Self::Timestamp { present, .. }
            | Self::Struct { present } => present,
            Self::Unsupported(note) => unimplemented!("{}", note),
        }
    }

    /// Private method used by from_json to coerce JSON values into arrow's split present stream
    ///
    /// Types:
    ///    In: The content of the input vector (maybe a JSON Value)
    ///    Out: The output of the transformation function (Maybe a primitive type like i64)
    ///    F: The function you're trying to cast with
    fn coerce_to_arrow_vecs<In, Out, F: Fn(&In) -> Option<Out>>(
        items: &[In],
        tran: F,
    ) -> (Option<Vec<bool>>, Vec<Out>) {
        // The present stream just records nulls separately from the data stream, easy.
        let coerced: Vec<_> = items.iter().map(tran).collect();
        let present: Vec<bool> = coerced.iter().map(|it| it.is_some()).collect();
        let present = if present.iter().all(|x| *x) {
            None
        } else {
            Some(present)
        };
        (present, coerced.into_iter().filter_map(|x| x).collect())
    }

    /// Private method used by several to_xyz type methods to coerce arrow's split present stream into a vector of options
    ///
    /// Types:
    ///    In: The content of the input vector (maybe a JSON Value)
    ///    Out: The output of the transformation function (Maybe a primitive type like i64)
    ///    F: The function you're trying to cast with
    fn coerce_to_opt_vecs<In, Out, F: Fn(&In) -> Option<Out>>(
        present: &Option<Vec<bool>>,
        items: &[In],
        tran: F,
    ) -> Vec<Option<Out>> {
        // If the present stream is Some(_) then it's length is the length of this stream.
        // If present is None, then items length is the length of this stream.
        match present {
            Some(ref present_vec) => {
                let mut item_idx = 0;
                present_vec
                    .iter()
                    .map(|&p| {
                        if p && item_idx < items.len() {
                            item_idx += 1;
                            tran(&items[item_idx - 1])
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            None => items.iter().map(tran).collect(),
        }
    }

    /// Read a column from a vector of JSON values
    ///
    /// This is infallible only because if there is a problem coercing a value, it's treated as null
    ///
    /// ```
    /// use serde_json::json;
    /// use orc_format::Column;
    /// let orig = &[json!(1), json!(2), json!(3)];
    /// assert_eq!(
    ///     Column::from_json(orig),
    ///     Column::from(vec![1i128, 2, 3])
    /// );
    /// ```
    pub fn from_json(items: &[json::Value]) -> Self {
        let type_guess = items
            .iter()
            .filter(|it| !it.is_null())
            .next()
            .cloned()
            .unwrap_or(json::Value::Bool(false));
        if type_guess.is_boolean() {
            let (present, data) = Self::coerce_to_arrow_vecs(items, |j| j.as_bool());
            Column::Boolean { present, data }
        } else if type_guess.is_i64() {
            let (present, data) =
                Self::coerce_to_arrow_vecs(items, |j| j.as_i64().map(|x| x as i128));
            Column::Int { present, data }
        } else if type_guess.is_f64() {
            let (present, data) = Self::coerce_to_arrow_vecs(items, |j| j.as_f64());
            Column::Double { present, data }
        } else {
            todo!("Unsupported JSON Column type yet: {:?}", type_guess);
        }
    }

    /// Convert to a vector of nullable booleans, if possible
    pub fn to_booleans(&self) -> Option<Vec<Option<bool>>> {
        match self {
            Column::Boolean { present, data } => {
                Some(Self::coerce_to_opt_vecs(present, data, |x| Some(*x)))
            }
            _ => None,
        }
    }

    /// Convert to a vector of nullable integers, if possible
    ///
    /// * The exact type is inferred, or you can override it.
    /// * Type conversion failures will result in None for that element.
    pub fn to_numbers<N: PrimNumCast>(&self) -> OrcResult<Vec<Option<N>>> {
        self.try_into()
    }

    /// Convert to a vector of non-nullable integers, if possible.
    ///
    /// * The exact numeric type is inferred, or you can override it.
    /// * This is a fast path for non-nullible columns, but it will fail (returning None)
    ///   if any nulls are present.
    /// * Type conversion failures will result in the whole column failing (with None)
    pub fn to_all_numbers<N: num_traits::NumCast>(&self) -> Option<Vec<N>> {
        self.try_into()
    }

    /// Split a concatenated array of blobs into a vector of optional slices
    ///
    /// This is used by as_byte_slices to handle the common parts of direct and dictionary encoding
    fn split_nullible_stream<'t, X>(
        present: &Option<Vec<bool>>,
        length: &Vec<u128>,
        data: &'t Vec<X>,
    ) -> Vec<Option<&'t [X]>> {
        let blob_count = present.as_ref().map(|p| p.len()).unwrap_or(length.len());
        let mut blobs: Vec<Option<&[X]>> = vec![];
        let mut byte_cursor = 0; // Where are we in the concatenated string
        for blob_ix in 0..blob_count {
            if blobs.len() < length.len()
                && (present.is_none() || present.as_ref().unwrap()[blob_ix])
            {
                let blob_len = length[blobs.len()] as usize;
                blobs.push(Some(&data[byte_cursor..byte_cursor + blob_len]));
                byte_cursor += blob_len;
            } else {
                blobs.push(None);
            }
        }
        blobs
    }

    /// View the column as a vector of byte slices, if it's possible.
    ///
    /// This is supported only for Blob columns, and for String columns
    /// which are also aliased as Char and Varchar.
    pub fn as_slices(&self) -> OrcResult<Vec<Option<&[u8]>>> {
        self.try_into()
    }

    /// Convert the column to a vector of blobs, if it's possible.
    ///
    /// This does a whole lot of allocation, so expect it to be slow. Prefer slices if possible.
    pub fn to_vecs(&self) -> OrcResult<Vec<Option<Vec<u8>>>> {
        self.try_into()
    }

    /// Convert the column to a vector of str, if possible.
    ///
    /// * This doesn't allocate but may still be fairly slow since it does check utf8 validity.
    /// * Since ORC requires UTF8 for strings, errors are unlikely unless you read blobs as strings.  
    /// * This is intended as a
    /// * If you need more speed, and you're sure you have only utf8,
    ///     consider using `as_byte_slices()` with `from_utf8_unchecked()`.
    /// * If you need more error granularity, use `as_byte_slices()` with `from_utf8()`.
    pub fn as_strs(&self) -> OrcResult<Vec<Option<&str>>> {
        self.try_into()
    }

    /// Convert the column to a vector of strings, if possible.
    ///
    /// This is based on `as_strs()` and inherits its tradeoffs.
    /// It will likely be slowe though, because it introduces a lot of allocation.
    pub fn to_strings(&self) -> OrcResult<Vec<Option<String>>> {
        self.try_into()
    }
}

/// View the column as a vector of byte slices, if it's possible.
///
/// This is supported only for Blob columns, and for String columns
/// which are also aliased as Char and Varchar.
impl<'t> TryFrom<&'t Column> for Vec<Option<&'t [u8]>> {
    type Error = OrcError;
    fn try_from(col: &'t Column) -> OrcResult<Self> {
        match col {
            // Blobs are stored concatenated, with a separate vector of lengths
            Column::Blob {
                length,
                data,
                present,
                ..
            } => Ok(Column::split_nullible_stream(present, length, data)),
            // Dictionary compressed blobs are stored as a sorted concatenated dictionary,
            // lengths of the keys of that dictionary in the same order,
            // and a vector of references into that dictionary
            Column::BlobDict {
                length,
                data,
                present,
                dictionary_data,
                ..
            } => {
                // First the dictionary is read like a standard string column
                let dictionary = Column::split_nullible_stream(&None, length, dictionary_data);

                // Then we decompress the rest using the dictionary
                let blob_count = present.as_ref().map(|p| p.len()).unwrap_or(data.len());
                let mut blobs: Vec<Option<&[u8]>> = vec![];
                for blob_ix in 0..blob_count {
                    if blobs.len() < data.len()
                        && (present.is_none() || present.as_ref().unwrap()[blob_ix])
                    {
                        blobs.push(dictionary[blobs.len()]);
                    } else {
                        blobs.push(None);
                    }
                }
                Ok(blobs)
            }
            _ => Err(OrcError::ColumnCastException(
                col.variant_name(),
                "Vec<Option<&[u8]>>",
            )), // These are not blobs
        }
    }
}

/// Convert the column to a vector of blobs, if it's possible.
///
/// This does a whole lot of allocation, so expect it to be slow. Prefer slices if possible.
impl TryFrom<&Column> for Vec<Option<Vec<u8>>> {
    type Error = OrcError;
    fn try_from(col: &Column) -> OrcResult<Self> {
        Ok(col
            .as_slices()?
            .into_iter()
            .map(|opt_slc| Some(opt_slc?.to_vec()))
            .collect())
    }
}

/// Convert the column to a vector of str, if possible.
///
/// * This doesn't allocate but may still be fairly slow since it does check utf8 validity.
/// * Since ORC requires UTF8 for strings, errors are unlikely unless you read blobs as strings.  
/// * This is intended as a
/// * If you need more speed, and you're sure you have only utf8,
///     consider using `as_byte_slices()` with `from_utf8_unchecked()`.
/// * If you need more error granularity, use `as_byte_slices()` with `from_utf8()`.
impl<'t> TryFrom<&'t Column> for Vec<Option<&'t str>> {
    type Error = OrcError;
    fn try_from(col: &'t Column) -> OrcResult<Self> {
        // This is a tad longer because it's clearer (to me) how the error propagates
        // TODO: We could avoid checking UTF8 for each string if it was dictionary compressed
        // TODO: Should we allow decoding Blobs as strings? We do now because we don't check Column::Blob::utf8
        let slices = col.as_slices()?;
        let mut col = vec![];
        for row in slices {
            col.push(match row {
                None => None,
                // Any failing string will halt decoding the column
                Some(slc) => Some(std::str::from_utf8(slc)?),
            });
        }
        Ok(col)
    }
}

/// Convert the column to a vector of blobs, if it's possible.
///
/// This does a whole lot of allocation, so expect it to be slow. Prefer slices if possible.
impl TryFrom<&Column> for Vec<Option<String>> {
    type Error = OrcError;
    fn try_from(col: &Column) -> OrcResult<Self> {
        Ok(col
            .as_strs()?
            .into_iter()
            .map(|op| op.map(|s| s.to_owned()))
            .collect())
    }
}

/// Local trait, to generalize over primitive numbers
/// It's necessary because remote traits could have a conflict in the future
/// In this case it wouldn't but the compiler doesn't know that.
pub trait PrimNumCast: num_traits::NumCast {}
impl PrimNumCast for u8 {}
impl PrimNumCast for i8 {}
impl PrimNumCast for u16 {}
impl PrimNumCast for i16 {}
impl PrimNumCast for u32 {}
impl PrimNumCast for i32 {}
impl PrimNumCast for u64 {}
impl PrimNumCast for i64 {}
impl PrimNumCast for usize {}
impl PrimNumCast for isize {}
impl PrimNumCast for f32 {}
impl PrimNumCast for f64 {}

/// Convert to a vector of nullable integers, if possible
///
/// * The exact type is inferred, or you can override it.
/// * Type conversion failures will result in None for that element.
impl<N: PrimNumCast> TryFrom<&Column> for Vec<Option<N>> {
    type Error = OrcError;
    fn try_from(col: &Column) -> OrcResult<Self> {
        match col {
            Column::Byte { present, data } => {
                Ok(Column::coerce_to_opt_vecs(present, data, |x| N::from(*x)))
            }
            Column::Int { present, data } => {
                Ok(Column::coerce_to_opt_vecs(present, data, |x| N::from(*x)))
            }
            Column::Float { present, data } => {
                Ok(Column::coerce_to_opt_vecs(present, data, |x| N::from(*x)))
            }
            Column::Double { present, data } => {
                Ok(Column::coerce_to_opt_vecs(present, data, |x| N::from(*x)))
            }
            _ => Err(OrcError::ColumnCastException(
                col.variant_name(),
                "a nullable numeric type",
            )),
        }
    }
}

/// Convert to a vector of non-nullable integers, if possible.
///
/// * The exact numeric type is inferred, or you can override it.
/// * This is a fast path for non-nullible columns, but it will fail
///   if any nulls are present.
/// * Type conversion failures will result in the whole column failing
impl<N: PrimNumCast> TryFrom<&Column> for Vec<N> {
    type Error = OrcError;
    fn try_from(col: &Column) -> OrcResult<Self> {
        if col.present().is_some() {
            // The present stream wasn't omitted so there must be a null somewhere
            return Err(OrcError::ColumnCastException(
                col.variant_name(),
                "a non-nullable numeric type (nulls are present)",
            ));
        }
        let conversion_failed = || {
            OrcError::ColumnCastException(
                col.variant_name(),
                "a nullable numeric type (invalid numeric conversion)",
            )
        };
        match col {
            Column::Byte { data, .. } => data
                .iter()
                .map(|x| N::from(*x).ok_or_else(conversion_failed))
                .collect(),
            Column::Int { data, .. } => data
                .iter()
                .map(|x| N::from(*x).ok_or_else(conversion_failed))
                .collect(),
            Column::Float { data, .. } => data
                .iter()
                .map(|x| N::from(*x).ok_or_else(conversion_failed))
                .collect(),
            Column::Double { data, .. } => data
                .iter()
                .map(|x| N::from(*x).ok_or_else(conversion_failed))
                .collect(),
            _ => Err(conversion_failed()),
        }
    }
}

// TODO: Write this more compactly
macro_rules! basic_column_from_vec {
    ($prim:ty, $constructor:path) => {
        impl From<Vec<$prim>> for Column {
            fn from(data: Vec<$prim>) -> Self {
                $constructor {
                    present: None,
                    data,
                }
            }
        }
        impl From<Vec<Option<$prim>>> for Column {
            fn from(data: Vec<Option<$prim>>) -> Self {
                let (present, data) =
                    Column::coerce_to_arrow_vecs(&data, |d| d.map(|x| x.clone().into()));
                $constructor { present, data }
            }
        }
    };
}
basic_column_from_vec!(bool, Column::Boolean);
basic_column_from_vec!(i128, Column::Int);

/// An ordered set of columns; a deserialized stripe or file
///
/// This is not a feature rich abstraction, just enough to make it easier to export
#[derive(Default, Debug, PartialEq)]
pub struct DataFrame {
    pub column_order: Vec<String>,
    pub columns: HashMap<String, Column>,
    pub length: usize,
}
impl DataFrame {
    /// Read a dataframe from newline separated JSON records. Column order is not preserved.
    ///
    /// Reading from a slice is fairly easy using std::io::Cursor if needed
    pub fn from_json_lines<F: Read>(reader: F) -> OrcResult<Self> {
        // This is written for simplicity rather than speed.
        // It buffers the json Value enums, which wastes space.
        // But it's intended for tests mostly, so speed should not be a problem.
        let line_reader = std::io::BufReader::new(reader).lines();

        let mut frame = HashMap::new();
        let mut header = true;
        for line in line_reader {
            let mut row: HashMap<String, json::Value> = json::from_str(&line?)?;
            if header {
                for column_name in row.keys() {
                    frame.insert(column_name.to_owned(), vec![]);
                }
                header = false;
            }
            for (name, column) in &mut frame {
                match row.remove(name) {
                    Some(cell) => column.push(cell),
                    None => column.push(json::Value::Null),
                }
            }
        }
        Ok(DataFrame {
            column_order: frame.keys().cloned().collect(),
            length: frame.values().next().map(|c| c.len()).unwrap_or(0),
            columns: frame
                .into_iter()
                .map(|(k, v)| (k, Column::from_json(&v)))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Column;
    #[test]
    fn test_column_coerce_vec() {
        // Mixed types
        let items = &[json!(true), json!(false), json!(null), json!("foo")];
        let (present, data) = Column::coerce_to_arrow_vecs(items, |j| j.as_bool());
        assert_eq!(present, Some(vec![true, true, false, false]));
        assert_eq!(data, vec![true, false]);

        // All present
        let items = &[json!(true), json!(false)];
        let (present, data) = Column::coerce_to_arrow_vecs(items, |j| j.as_bool());
        assert_eq!(present, None);
        assert_eq!(data, vec![true, false]);
    }
    #[test]
    fn column_from_json() {
        let orig = &[json!(1), json!(2), json!(3)];
        assert_eq!(
            Column::from_json(orig).to_all_numbers(),
            Some(vec![1, 2, 3])
        );
    }
}
