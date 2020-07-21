use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrcError {
    #[error("Malformed protobuf message; the data is probably corrupt.")]
    ProtobufError(#[from] prost::DecodeError),
    #[error("Truncated or empty message. Is the file empty or incomplete?")]
    TruncatedError,
}

pub type OrcResult<T> = std::result::Result<T, OrcError>;