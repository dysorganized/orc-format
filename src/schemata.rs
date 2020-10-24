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

/// A vector of nullable elements where only the valid elements are stored
///
/// Note this is different from a masked column, where both empry and filled values are stored
/// and the empty cells are merely excluded from some computations
#[derive(Debug, PartialEq, Clone)]
pub struct NullableColumn {
    present: Option<Vec<bool>>,
    content: Column,
    length: usize,
}
impl NullableColumn {
    /// Read a nullable column from a stripe
    pub(crate) fn from_stripe<F: Read + Seek>(
        stripe: &Stripe,
        schema: &Schema,
        enc: &messages::ColumnEncoding,
        streams: &HashMap<(u32, messages::stream::Kind), std::ops::Range<u64>>,
        orc: &mut ORCFile<F>,
    ) -> OrcResult<NullableColumn> {
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
        let present = slice_by_kind(Skind::Present)
            .and_then(|r| {
                codecs::BooleanRLEDecoder::from(&r[..])
                    .take(stripe.rows())
                    .collect::<OrcResult<Vec<bool>>>()
            })
            .ok();

        Ok(NullableColumn::new(
            stripe.rows(),
            present,
            Column::new(stripe, schema, enc, streams, orc)
                .unwrap_or_else(|er| Column::Unsupported(er.to_string())),
        ))
    }

    /// Create a new NullableColumn.
    /// Uses an optional mask, which if provided, will override length.
    /// If the mask is missing, it will be treated like `Some(vec![true; length])`
    /// The provided content column contains *only the non-null entries*
    pub fn new(mut length: usize, present: Option<Vec<bool>>, content: Column) -> Self {
        let present = match present {
            None => None, // Completely filled vector
            Some(v) => {
                if v.is_empty() {
                    None // Still completely filled
                } else if v.iter().all(|v| *v) {
                    None // Still filled
                } else {
                    length = v.len();
                    Some(v) // Looks like it has a null
                }
            }
        };
        Self {
            present,
            content,
            length,
        }
    }

    /// Get a slice of booleans indicating whether each element is non-null
    /// It's optional because if the vector has no nulls (is full), the vector is absent
    pub fn present(&self) -> Option<&[bool]> {
        self.present.as_ref().map(|x| &x[..self.length])
    }

    /// Get a vector of vooleans that represent whether each element is non-null
    /// If the vector is missing (meaning all values are present) then create a new
    /// vector of all trues. This is somewhat expensive so only use this sparingly.
    pub fn present_or_trues(&self) -> Vec<bool> {
        match &self.present {
            &Some(ref v) => v[..self.length].to_vec(),
            &None => (0..self.length).map(|_| true).collect(),
        }
    }

    /// Get only the valid entries. If this column has no nulls, this is the whole column
    pub fn content(&self) -> &Column {
        &self.content
    }

    /// Fill out the content to have default values where there are nulls
    /// This is useful for masked arrays like in Python, but it's not cheap.
    /// This will make a copy of the original column
    pub fn content_or_default(&self) -> Column {
        self.content.clone().expand(self.present())
    }

    /// Get how many elements there are in this column.
    /// It should be the same for all columns in a stripe.
    pub fn length(&self) -> usize {
        // This was previously necessary but may be pointless now.
        // Boolean vectors are padded to multiples of eight on account of bit packing,
        // so present.len() was not accurate. It may always be accurate now that the
        // present vectors are trimmed
        self.length
    }

    /// Read a column from a vector of JSON values
    ///
    /// This is infallible only because if there is a problem coercing a value, it's treated as null
    ///
    /// ```
    /// use serde_json::json;
    /// use orc_format::Column;
    /// let orig = &[json!(1), json!(null), json!(3)];
    /// assert_eq!(
    ///     NullableColumn::from_json(orig),
    ///     NullableColumn::from(vec![1i128, 2, 3])
    /// );
    /// ```
    pub fn from_json(items: &[json::Value]) -> Self {
        let present: Vec<_> = items.iter().map(|v| !v.is_null()).collect();
        Self::new(present.len(), Some(present), Column::from_json(items))
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
#[derive(Debug, PartialEq, Clone)]
pub enum Column {
    Boolean {
        data: Vec<bool>,
    },
    Byte {
        data: Vec<u8>,
    },
    Int {
        data: Vec<i128>,
    },
    Float {
        data: Vec<f32>,
    },
    Double {
        data: Vec<f64>,
    },

    /// A vector of strings, stored as a concatenated 1D array, and an array of lengths  
    Blob {
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
        data: Vec<u128>,
        dictionary_data: Vec<u8>,
        length: Vec<u128>,
        utf8: bool,
    },

    /// A list, like a blob, is a length vector and a concatenated array of elements
    List {
        length: Vec<u128>,
        elements: Box<Column>,
    },
    /// A map is like two aligned List columns
    Map {
        length: Vec<u128>,
        keys: Box<Column>,
        values: Box<Column>,
    },

    /// Decimals, which store i128's with an associated scale.
    Decimal {
        data: Vec<i128>,
        scale: Vec<i128>,
    },
    /// Timestamps, which store time since 1970, plus a second stream with nanoseconds
    Timestamp {
        seconds: Vec<i128>,
        nanos: Vec<u128>,
    },
    /// Structs only store if they are present. The rest is in other columns.
    Struct {
        fields: Vec<(String, Column)>,
    },
    // Unions are not supported.
    Unsupported(String),
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
                data: codecs::BooleanRLEDecoder::from(&data?[..])
                    // Booleans need to be trimmed because they round up to the nearest 8
                    .take(stripe.rows() as usize)
                    .collect::<OrcResult<_>>()?,
            },

            // Byte has it's own decoder
            Schema::Byte { .. } => Column::Byte {
                data: codecs::ByteRLEDecoder::from(&data?[..]).collect::<OrcResult<_>>()?,
            },

            // All the integers work the same
            // We promote them all to i128 for simplicity as a POC
            // TODO: Specialize the integers
            Schema::Short { .. }
            | Schema::Int { .. }
            | Schema::Long { .. }
            | Schema::Date { .. } => Column::Int {
                data: int_enc(data?)?,
            },

            // Float32, which has it's own decoder (the bit length is part of the encoder)
            Schema::Float { .. } => Column::Float {
                data: codecs::FloatDecoder::from(&data?[..]).collect::<OrcResult<_>>()?,
            },

            // Float64 has it's own decoder too
            Schema::Double { .. } => Column::Double {
                data: codecs::DoubleDecoder::from(&data?[..]).collect::<OrcResult<_>>()?,
            },

            // Decimals are huge ints with a scale, really just a knockoff IEEE754 again
            Schema::Decimal { .. } => Column::Decimal {
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
                        data: data?.to_vec(),
                        length: uint_enc(len?)?,
                        utf8,
                    },
                    Ckind::Dictionary | Ckind::DictionaryV2 => Column::BlobDict {
                        data: uint_enc(data?)?,
                        dictionary_data: slice_by_kind(Skind::DictionaryData)?.to_vec(),
                        length: uint_enc(len?)?,
                        utf8,
                    },
                }
            }
            Schema::Map { key, value, .. } => Column::Map {
                length: uint_enc(len?)?,
                keys: Box::new(Column::new(stripe, key, enc, streams, orc)?),
                values: Box::new(Column::new(stripe, value, enc, streams, orc)?),
            },
            Schema::List { inner, .. } => Column::List {
                length: uint_enc(len?)?,
                elements: Box::new(Column::new(stripe, inner, enc, streams, orc)?),
            },
            Schema::Struct { fields, .. } => Column::Struct {
                // Structs have any number of fields and any number can fail
                // Hence the collect and ?
                fields: fields
                    .iter()
                    .map(|(name, sch)| {
                        Ok((
                            name.to_string(),
                            Column::new(stripe, sch, enc, streams, orc)?,
                        ))
                    })
                    .collect::<OrcResult<_>>()?,
            },
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
                Column::Unsupported(
                    "Timestamp decoding is not supported yet for Column decoding".into(),
                )
            }
            Schema::Union { .. } => {
                Column::Unsupported("Union columns are not well defined or supported".into())
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
            Column::Boolean {
                data: items.into_iter().filter_map(|j| j.as_bool()).collect(),
            }
        } else if type_guess.is_i64() {
            Column::Int {
                data: items
                    .into_iter()
                    .filter_map(|j| j.as_i64().map(|x| x as i128))
                    .collect(),
            }
        } else if type_guess.is_f64() {
            Column::Double {
                data: items.into_iter().filter_map(|j| j.as_f64()).collect(),
            }
        } else {
            todo!("Unsupported JSON Column type yet: {:?}", type_guess);
        }
    }

    /// Convert to a vector of nullable booleans, if possible
    pub fn to_booleans(&self) -> Option<Vec<bool>> {
        match self {
            Column::Boolean { data } => Some(data.clone()),
            _ => None,
        }
    }

    /// Convert to a vector of non-nullable integers, if possible.
    ///
    /// * The exact numeric type is inferred, or you can override it.
    /// * This is a fast path for non-nullible columns, but it will fail (returning None)
    ///   if any nulls are present.
    /// * Type conversion failures will result in the whole column failing (with None)
    pub fn to_numbers<N: PrimNumCast>(&self) -> OrcResult<Vec<N>> {
        self.try_into()
    }

    /// Split a concatenated array of blobs into a vector of optional slices
    ///
    /// This is used by as_byte_slices to handle the common parts of direct and dictionary encoding
    fn split_stream<'t, X>(length: &Vec<u128>, data: &'t Vec<X>) -> Vec<&'t [X]> {
        let mut blobs: Vec<&[X]> = vec![];
        let mut byte_cursor = 0; // Where are we in the concatenated string
        for blob_len in length {
            blobs.push(&data[byte_cursor..byte_cursor + *blob_len as usize]);
            byte_cursor += *blob_len as usize;
        }
        blobs
    }

    /// View the column as a vector of byte slices, if it's possible.
    ///
    /// This is supported only for Blob columns, and for String columns
    /// which are also aliased as Char and Varchar.
    pub fn as_slices(&self) -> OrcResult<Vec<&[u8]>> {
        self.try_into()
    }

    /// Convert the column to a vector of blobs, if it's possible.
    ///
    /// This does a whole lot of allocation, so expect it to be slow. Prefer slices if possible.
    pub fn to_vecs(&self) -> OrcResult<Vec<Vec<u8>>> {
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
    pub fn as_strs(&self) -> OrcResult<Vec<&str>> {
        self.try_into()
    }

    /// Convert the column to a vector of strings, if possible.
    ///
    /// This is based on `as_strs()` and inherits its tradeoffs.
    /// It will likely be slowe though, because it introduces a lot of allocation.
    pub fn to_strings(&self) -> OrcResult<Vec<String>> {
        self.try_into()
    }

    fn expand_inner<X: Default + Copy>(present: &[bool], src: &[X]) -> Vec<X> {
        present.iter()
        .scan((0, false), |(ix, _), &p| if p {
            *ix += 1;
            Some(Some(*ix-1))
        } else {
            Some(None)
        })
        .map(|ix| ix.map(|i| src[i]).unwrap_or_default())
        .collect()
    }

    /// Expand the column, filling nulls with a default value
    /// Column doesn't naturally have nulls so normally you should call
    /// `NullableColumn.content_or_default()` instead.
    /// But you can provide an array of nulls if you want.
    pub fn expand(self, present: Option<&[bool]>) -> Column {
        let present = match present {
            Some(x) => x,
            None => return self.to_owned()
        };
        
        match self {
            // Primitives use defaults,
            // but variable length columns like strings usually don't require much change,
            // only the length column needs to be padded.
            Column::Boolean { data} => Column::Boolean {
                data: Self::expand_inner(present, &data)
            },
            Column::Byte { data} => Column::Byte {
                data: Self::expand_inner(present, &data)
            },
            Column::Int { data} => Column::Int {
                data: Self::expand_inner(present, &data)
            },
            Column::Float { data} => Column::Float {
                data: Self::expand_inner(present, &data)
            },
            Column::Double { data} => Column::Double {
                data: Self::expand_inner(present, &data)
            },
            Column::Blob { data, length, utf8 } => Column::Blob {
                data,
                length: Self::expand_inner(present, &length),
                utf8
            },
            Column::List { length, elements} => Column::List {
                length: Self::expand_inner(present, &length),
                elements
            },
            Column::Map { length, keys, values} => Column::Map {
                length: Self::expand_inner(present, &length),
                keys,
                values
            },
            Column::Struct { .. } => todo!("Expanding struct columns is not implemented yet"),
            Column::Unsupported(note) => Column::Unsupported(note),
            _ => todo!("Can't extend this column type yet")
        }
    }
}

/// View the column as a vector of byte slices, if it's possible.
///
/// This is supported only for Blob columns, and for String columns
/// which are also aliased as Char and Varchar.
impl<'t> TryFrom<&'t Column> for Vec<&'t [u8]> {
    type Error = OrcError;
    fn try_from(col: &'t Column) -> OrcResult<Self> {
        match col {
            // Blobs are stored concatenated, with a separate vector of lengths
            Column::Blob { length, data, .. } => Ok(Column::split_stream(length, data)),
            // Dictionary compressed blobs are stored as a sorted concatenated dictionary,
            // lengths of the keys of that dictionary in the same order,
            // and a vector of references into that dictionary
            Column::BlobDict {
                length,
                data,
                dictionary_data,
                ..
            } => {
                // First the dictionary is read like a standard string column
                let dictionary = Column::split_stream(length, dictionary_data);

                // Then we decompress the rest using the dictionary
                let mut blobs: Vec<&[u8]> = vec![];
                for blob_ix in 0..data.len() {
                    let dict_ix = data[blob_ix] as usize;
                    if dict_ix < dictionary.len() {
                        blobs.push(dictionary[dict_ix]);
                    } else {
                        return Err(OrcError::TruncatedError(
                            "Dictionary encoded column references a value outside the dictionary",
                        ));
                    }
                }
                Ok(blobs)
            }
            _ => Err(OrcError::ColumnCastException(
                col.variant_name(),
                "Vec<&[u8]>",
            )), // These are not blobs
        }
    }
}

/// Convert the column to a vector of blobs, if it's possible.
///
/// This does a whole lot of allocation, so expect it to be slow. Prefer slices if possible.
impl TryFrom<&Column> for Vec<Vec<u8>> {
    type Error = OrcError;
    fn try_from(col: &Column) -> OrcResult<Self> {
        Ok(col
            .as_slices()?
            .into_iter()
            .map(|slc| slc.to_vec())
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
impl<'t> TryFrom<&'t Column> for Vec<&'t str> {
    type Error = OrcError;
    fn try_from(col: &'t Column) -> OrcResult<Self> {
        // This is a tad longer because it's clearer (to me) how the error propagates
        // TODO: We could avoid checking UTF8 for each string if it was dictionary compressed
        // TODO: Should we allow decoding Blobs as strings? We do now because we don't check Column::Blob::utf8
        let slices = col.as_slices()?;
        let mut col = vec![];
        for row in slices {
            // Any failing string will halt decoding the column
            col.push(std::str::from_utf8(row)?);
        }
        Ok(col)
    }
}

/// Convert the column to a vector of blobs, if it's possible.
///
/// This does a whole lot of allocation, so expect it to be slow. Prefer slices if possible.
impl TryFrom<&Column> for Vec<String> {
    type Error = OrcError;
    fn try_from(col: &Column) -> OrcResult<Self> {
        Ok(col.as_strs()?.into_iter().map(|s| s.to_owned()).collect())
    }
}

/// Local trait, to generalize over primitive numbers
/// It's necessary because remote traits could have a conflict in the future
/// In this case it wouldn't but the compiler doesn't know that.
pub trait PrimNumCast: num_traits::NumCast + std::str::FromStr {}
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

/// Convert to a vector of non-nullable integers, if possible.
///
/// * The exact numeric type is inferred, or you can override it.
/// * This is a fast path for non-nullible columns, but it will fail
///   if any nulls are present.
/// * Type conversion failures will result in the whole column failing
impl<N: PrimNumCast> TryFrom<&Column> for Vec<N> {
    type Error = OrcError;
    fn try_from(col: &Column) -> OrcResult<Self> {
        let conversion_failed = || {
            OrcError::ColumnCastException(
                col.variant_name(),
                "a nullable numeric type (invalid numeric conversion)",
            )
        };
        match col {
            Column::Boolean { data, .. } => data
                .iter()
                .map(|x| N::from(if *x { 1 } else { 0 }).ok_or_else(conversion_failed))
                .collect(),
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

/// An ordered set of columns; a deserialized stripe or file
///
/// This is not a feature rich abstraction, just enough to make it easier to export
#[derive(Default, Debug, PartialEq)]
pub struct DataFrame {
    pub column_order: Vec<String>,
    pub columns: HashMap<String, NullableColumn>,
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
                .map(|(k, v)| (k, NullableColumn::from_json(&v)))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Column;
    #[test]
    fn column_from_json() {
        let orig = &[json!(1), json!(2), json!(3)];
        assert_eq!(
            Column::from_json(orig).to_numbers::<i64>().unwrap(),
            vec![1, 2, 3]
        );
    }
}
