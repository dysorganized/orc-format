use std::collections::HashMap;
use std::io::{BufRead, Read, Seek};
use bytes::Bytes;
use serde_json as json;
use crate::codecs;
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

/// Composite types made from multiple streams.
///
/// These are analogous to Arrow formats, but not quite - specifically these are mutable.
#[derive(Debug, PartialEq)]
pub enum Column {
    /// Boolean RLE
    Boolean {present: Option<Vec<bool>>, data: Vec<bool>},
    /// RLE encoding for 8-bit integers and UNIONS. Unions aren't supported.
    Byte {present: Option<Vec<bool>>, data: Vec<u8>},
    /// `DIRECT` encoding, for all int sizes, booleans, bytes, dates.
    /// For dates, the data is the days since 1970.  
    Int {present: Option<Vec<bool>>, data: Vec<i128>},
    /// `DIRECT` encoding for floats
    Float {present: Option<Vec<bool>>, data: Vec<f64>},
    /// `DIRECT` encoding for chars, strings, varchars, and blobs
    Blob {present: Option<Vec<bool>>, data: Vec<u8>, length: Vec<u128>},
    /// For maps and lists, the data is the length.
    Container {present: Option<Vec<bool>>, length: Vec<u128>},
    /// `DICTIONARY` encoding, only for chars, strings and varchars (not blobs)
    Dict {present: Option<Vec<bool>>, data: Vec<u128>, length: Vec<u128>, dict: Vec<u8>},
    /// Decimals, which store i128's with an associated scale.
    Decimal {present: Option<Vec<bool>>, data: Vec<i128>, scale: Vec<i128> },
    /// Timestamps, which store time since 1970, plus a second stream with nanoseconds
    Timestamp {present: Option<Vec<bool>>, seconds: Vec<i128>, nanos: Vec<u128> },
    /// Structs only store if they are present. The rest is in other columns.
    Struct {present: Option<Vec<bool>>},
    // Unions are not supported.
}

impl Column {
    pub fn new<F: Read+Seek>(
        stripe: &Stripe,
        schema: &Schema,
        enc: &messages::ColumnEncoding,
        streams: &HashMap<(u32, messages::stream::Kind), std::ops::Range<u64>>,
        orc: &mut ORCFile<F>
    ) -> OrcResult<Column> {
        use messages::column_encoding::Kind as Ckind;
        use messages::stream::Kind as Skind;
        let mut slice_by_kind = |sk: Skind| streams
            .get(&(schema.id() as u32, sk))
            .cloned()
            .ok_or(OrcError::EncodingError(format!("Column {} missing {:?} stream", schema.id(), sk)))
            .and_then(|rng| orc.read_compressed(
                stripe.info.offset() + rng.start
                .. stripe.info.offset() + rng.end
            ));
        // Most colspecs need these streams, so tee them up to save writing
        let data = slice_by_kind(Skind::Data);
        let len = slice_by_kind(Skind::Length);
        let present = slice_by_kind(Skind::Present)
                .and_then(|r| codecs::BooleanRLEDecoder::from(&r[..])
                    .collect::<OrcResult<Vec<bool>>>())
                .ok();

        // These integer encodings are mostly orthogonal to the types
        let int_enc = |rs: Bytes| match enc.kind() {
            Ckind::Direct | Ckind::Dictionary => codecs::RLE1::<i128>::from(&rs[..]).collect::<OrcResult<Vec<i128>>>(),
            Ckind::DirectV2 | Ckind::DictionaryV2 => codecs::RLE2::<i128>::from(&rs[..]).collect::<OrcResult<Vec<i128>>>()
        };
        let uint_enc = |rs: Bytes| match enc.kind() {
            Ckind::Direct | Ckind::Dictionary => codecs::RLE1::<u128>::from(&rs[..]).collect::<OrcResult<Vec<u128>>>(),
            Ckind::DirectV2 | Ckind::DictionaryV2 => codecs::RLE2::<u128>::from(&rs[..]).collect::<OrcResult<Vec<u128>>>()
        };
        let content = match schema {
            Schema::Boolean{..} => Column::Boolean{
                present,
                data: codecs::BooleanRLEDecoder::from(&data?[..])
                // Booleans need to be trimmed because they round up to the nearest 8
                .take(stripe.info.number_of_rows() as usize)
                .collect::<OrcResult<_>>()?
            },
            Schema::Byte{..}   => Column::Byte{
                present,
                data: codecs::ByteRLEDecoder::from(&data?[..])
                .collect::<OrcResult<_>>()?
            },
            Schema::Short{..}
            | Schema::Int{..}
            | Schema::Long{..}
            | Schema::Date{..} => Column::Int{present, data: int_enc(data?)?},
            Schema::Float{..}
            | Schema::Double{..} => {
                // It would look like this
                // ColumnContent::Float{data: FloatDecoder.from(data?)?}
                todo!("Float decoding isn't supported yet for Column decoding")
            },
            Schema::Decimal{..} => Column::Decimal{
                present,
                data: int_enc(data?)?,
                scale: int_enc(slice_by_kind(Skind::Secondary)?)?
            },
            Schema::String{..}
            | Schema::Varchar{..}
            | Schema::Char{..}
            | Schema::Binary{..} => Column::Blob{
                present,
                data: codecs::ByteRLEDecoder::from(&data?[..])
                    .collect::<OrcResult<_>>()?,
                length: uint_enc(len?)?
            },
            Schema::Map{..}
            | Schema::List{..} => Column::Container{present, length: uint_enc(len?)?},
            Schema::Struct{..} => Column::Struct{present},
            Schema::Timestamp{..} => {
                // It could look something like this:
                // ColumnContent::Timestamp{
                //     seconds: int_enc(data?),
                //     nanos: match enc.kind() {
                //         Ckind::Direct => Codec::NanosEnc1.decode(range_by_kind(Skind::Secondary)?),
                //         Ckind::DirectV2 => Codec::NanosEnc2.decode(range_by_kind(Skind::Secondary)?),
                //         _ => return Err(OrcError::EncodingError(format!("Timestamp doesn't support dictionary encoding")))
                //     }
                // }
                todo!("Timestamp decoding is not supported yet for Column decoding")
            },
            Schema::Union{..} => return Err(OrcError::SchemaError("Union types are not supported")),
        };
        Ok(content)
    }

    /// A stream indicating whether the row is non-null
    /// 
    /// If it's empty (e.g. None, not length 0) then all rows are present.
    /// Keep in mind it's frequently the case all rows are present so expect None
    pub fn present(&self) -> &Option<Vec<bool>> {
        match self {
            Self::Boolean {present, ..}
            | Self::Byte {present, ..}
            | Self::Int {present, ..}
            | Self::Float {present, ..}
            | Self::Blob {present, ..}
            | Self::Container {present, ..}
            | Self::Dict {present, ..}
            | Self::Decimal {present, ..}
            | Self::Timestamp {present, ..}
            | Self::Struct {present} => present
        }
    }

    /// Private method used by from_json to coerce JSON values into static types
    fn coerce_vec<In, Out, F: Fn(&In) -> Option<Out>>(items: &[In], tran: F) -> (Option<Vec<bool>>, Vec<Out>) {
        // The present stream just records nulls separately from the data stream, easy.
        let coerced : Vec<_> = items.iter()
            .map(tran)
            .collect();
        let present: Vec<bool> = coerced.iter().map(|it| it.is_some() ).collect();
        let present = if present.iter().all(|x| *x) {
            None
        } else {
            Some(present)
        };
        (present, coerced.into_iter().filter_map(|x| x).collect())
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
            .filter(|it| !it.is_null() )
            .next()
            .cloned()
            .unwrap_or(json::Value::Bool(false));
        if type_guess.is_boolean() {
            let (present, data) = Self::coerce_vec(items, |j| j.as_bool());
            Column::Boolean { present, data }
        } else if type_guess.is_i64() {
            let (present, data) = Self::coerce_vec(items, |j| j.as_i64().map(|x| x as i128));
            Column::Int { present, data }
        } else if type_guess.is_f64() {
            let (present, data) = Self::coerce_vec(items, |j| j.as_f64());
            Column::Float { present, data }
        } else {
            todo!("Unsupported JSON Column type yet: {:?}", type_guess);
        }
    }
}
// TODO: Write this more compactly
macro_rules! basic_column_from_vec {
    ($prim:ty, $constructor:path) => {
        impl From<Vec<$prim>> for Column {
            fn from(data: Vec<$prim>) -> Self {
                $constructor { present: None, data }
            }
        }
        impl From<Vec<Option<$prim>>> for Column {
            fn from(data: Vec<Option<$prim>>) -> Self {
                let (present, data) = Column::coerce_vec(&data, |d| d.map(|x| x.clone().into()));
                $constructor { present, data }
            }
        }
    }
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
    pub length: usize
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
                    None => column.push(json::Value::Null)
                }
            }
        }
        Ok(DataFrame {
            column_order: frame.keys().cloned().collect(),
            length: frame.values().next().map(|c| c.len()).unwrap_or(0),
            columns: frame.into_iter().map(|(k, v)| (k, Column::from_json(&v))).collect()
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
        let (present, data) = Column::coerce_vec(items, |j| j.as_bool());
        assert_eq!(present, Some(vec![true, true, false, false]));
        assert_eq!(data , vec![true, false]);

        // All present
        let items = &[json!(true), json!(false)];
        let (present, data) = Column::coerce_vec(items, |j| j.as_bool());
        assert_eq!(present, None);
        assert_eq!(data , vec![true, false]);
    }
    #[test]
    fn column_from_json() {
        let orig = &[json!(1), json!(2), json!(3)];
        Column::from_json(orig);
    }
}