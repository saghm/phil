use err_derive::Error;
use mongodb::bson::Document;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(display = "error when adding shard: {}", response)]
    AddShardError { response: Document },

    #[error(display = "{}", inner)]
    BsonDecoder {
        #[error(cause)]
        inner: mongodb::bson::DecoderError,
    },

    #[error(display = "{}", inner)]
    Monger {
        #[error(cause)]
        inner: monger_core::error::Error,
    },

    #[error(display = "{}", inner)]
    Mongo {
        #[error(cause)]
        inner: mongodb::error::Error,
    },

    #[error(display = "error when configuring replica set: {}", response)]
    ReplicaSetConfigError { response: Document },
}

macro_rules! define_error_from {
    ($ext:ty, $var:ident) => {
        impl From<$ext> for Error {
            fn from(err: $ext) -> Self {
                Error::$var { inner: err }
            }
        }
    };
}

define_error_from!(monger_core::error::Error, Monger);
define_error_from!(mongodb::bson::DecoderError, BsonDecoder);
define_error_from!(mongodb::error::Error, Mongo);
