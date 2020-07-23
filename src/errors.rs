use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrcError {
    #[error("Malformed protobuf message; the data is probably corrupt.")]
    ProtobufError(#[from] prost::DecodeError),
    #[error("Truncated or empty message. Is the file empty or incomplete?")]
    TruncatedError,
    #[error("Invalid schema: {0}")]
    SchemaError(&'static str),
    #[error("No such stripe: {0}")]
    NoSuchStripe(usize),
    #[error("IO Error")]
    IOError(#[from] std::io::Error)
}

pub type OrcResult<T> = std::result::Result<T, OrcError>;