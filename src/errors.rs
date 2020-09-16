use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrcError {
    #[error("Malformed protobuf message; the data is probably corrupt.")]
    ProtobufError(#[from] prost::DecodeError),
    #[error("Reached end of buffer while reading {0}")]
    TruncatedError(&'static str),
    #[error("Invalid schema: {0}")]
    SchemaError(&'static str),
    #[error("Invalid encoding: {0}")]
    EncodingError(String),
    #[error("No such stripe: {0}")]
    NoSuchStripe(usize),
    #[error("IO Error")]
    IOError(#[from] std::io::Error),
    #[error("End of stream")]
    EndOfStream,
    #[error("Bit sequence was too long for the target type. Are you reading a literal over 4GB on a 32bit machine?")]
    LongBitstring
}

pub type OrcResult<T> = std::result::Result<T, OrcError>;