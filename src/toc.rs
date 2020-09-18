use std::io::{Read, Seek, SeekFrom, Cursor};
use std::collections::HashMap;
use prost::Message;
use bytes::Bytes;
use crate::messages;
use crate::schemata::{Schema, Column, DataFrame};
use crate::errors::{OrcError, OrcResult};

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
    /// Read a compressed Protobuf message from the file
    fn read_message<M: Message+Default>(&mut self, rng: std::ops::Range<u64>) -> OrcResult<M> {
        Ok(M::decode(self.read_compressed(rng)?)?)
    }

    /// Read a compressed bytestring from the ORC file
    ///
    /// Start and end are relative to the beginning of the file.
    pub(crate) fn read_compressed(&mut self, rng: std::ops::Range<u64>) -> OrcResult<Bytes> {
        // Read the compressed data from the file first
        self.file.seek(SeekFrom::Start(rng.start))?;
        let mut comp_buffer_back = vec![0u8; (rng.end - rng.start) as usize];
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
        Ok(Bytes::from(decomp_buffer))
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
            return Err(OrcError::TruncatedError("Empty file"))
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
        
        me.footer = me.read_message::<messages::Footer>(footer_start..postscript_start)?;
        me.metadata = me.read_message::<messages::Metadata>(metadata_start..footer_start)?;
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

    /// Get information about one stripe in the file.
    ///
    /// This only includes the metadata necessary to read the stripe, but doesn't retain a reference
    /// to the original file (to make ownership with threads easier)
    pub fn stripe(&mut self, stripe_id: usize) -> OrcResult<Stripe> {
        if stripe_id < self.footer.stripes.len() {
            Stripe::new(stripe_id,self)
        } else {
            Err(OrcError::NoSuchStripe(stripe_id))
        }
    }

    /// Get the number of stripes in this file
    pub fn stripe_count(&self) -> usize {
        self.footer.stripes.len()
    }

    /// Consume the file, iterating over all stripes, yielding dataframes
    pub fn dataframes(mut self) -> impl Iterator<Item=OrcResult<DataFrame>> {
        (0..self.stripe_count())
        .map(move |stripe_id: usize| self.stripe(stripe_id).and_then(|st| st.dataframe(&mut self)))
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
        None => Err(OrcError::TruncatedError("ORC file is empty")),
        Some(&length) => {
            let postscript_bytes = &byt[
                byt.len() - length as usize - 1 .. byt.len()-1];
            Ok(messages::PostScript::decode(postscript_bytes)?)
        }
    }
}

/// Metadata about a single stripe in an ORC file
#[derive(Debug, PartialEq)]
pub struct Stripe {
    pub(crate) info: messages::StripeInformation,
    pub(crate) footer: messages::StripeFooter,
    pub(crate) flat_schema: Vec<Schema>,
    pub(crate) streams: HashMap<(u32, messages::stream::Kind), std::ops::Range<u64>>
}
impl Stripe {
    /// Create a stripe from StripeInformation and a StripeFooter
    ///
    /// The StripeFooter is read from the parent file
    pub(crate) fn new<F: Read+Seek>(id: usize, file: &mut ORCFile<F>) -> OrcResult<Self> {
        let info = file.footer.stripes[id].clone();
        let footer: messages::StripeFooter = file.read_message(
            info.offset() + info.index_length() + info.data_length()
            .. 
            info.offset() + info.index_length() + info.data_length() + info.footer_length()
        )?;
        // Columns may need to search for several streams, and there may be many columns
        // So save some compute by indexing it.
        let streams = footer
            .streams
            .iter()
            .scan(0u64, |cur, st| {
                // The offset of each stream is the cumulative sum of the lengths
                let start = *cur;
                *cur += st.length();
                Some(((st.column(), st.kind()), start..*cur))
            })
            .collect();
        Ok(Stripe {
            info,
            footer,
            flat_schema: file.flat_schema().to_vec(),
            streams
        })
    }

    /// Number of rows in this stripe
    pub fn rows(&self) -> usize {
        self.info.number_of_rows() as usize
    }

    /// Number of top-level columns in the schema associated with this stripe
    ///
    /// This is the same for every stripe but it's convenient to have handy
    ///
    /// Panics if the ORC file doesn't have a top-level struct
    pub fn cols(&self) -> usize {
        match &self.flat_schema[0] {
            Schema::Struct{fields, ..} => fields.len(),
            _ => panic!("This non-standard ORC doesn't have a top level schema.")
        }
    }

    /// Read and return one column from the stripe
    pub fn column<F: Read+Seek>(&self, id: usize, toc: &mut ORCFile<F>) -> OrcResult<Column> {
        if self.footer.columns.len() != self.flat_schema.len() {
            return Err(OrcError::SchemaError("Stripe column definitions don't match schema"))
        }
        Column::new(
            self,
            &self.flat_schema[id], 
            &self.footer.columns[id], 
            &self.streams,
            toc
        )
    }

    /// Deserialize the stripe into a basic dataframe
    pub fn dataframe<F: Read + Seek>(&self, toc: &mut ORCFile<F>) -> OrcResult<DataFrame> {
        let mut columns = HashMap::new();
        let mut column_order = vec![];
        if let Schema::Struct{fields, ..} = &self.flat_schema[0] {
            for (name, field) in fields {
                column_order.push(name.clone());
                columns.insert(name.clone(), Column::new(
                    self,
                    &self.flat_schema[field.id()], 
                    &self.footer.columns[field.id()], 
                    &self.streams,
                    toc
                )?);
            }
            Ok(DataFrame {column_order, columns, length: self.rows() })
        } else {
            panic!("This non-standard ORC doesn't have a top level schema. Can't create a dataframe.")
        }
    }
}