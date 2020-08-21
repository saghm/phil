use mongodb::bson::Document;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error when adding shard: {response}")]
    AddShardError { response: Document },

    #[error("{inner}")]
    BsonDecoder {
        #[from]
        inner: mongodb::bson::de::Error,
    },

    #[error("{inner}")]
    Io {
        #[from]
        inner: std::io::Error,
    },

    #[error("{inner}")]
    Monger {
        #[from]
        inner: monger_core::error::Error,
    },

    #[error("{inner}")]
    Mongo {
        #[from]
        inner: mongodb::error::Error,
    },

    #[error("error when configuring replica set: {response}")]
    ReplicaSetConfigError { response: Document },
}
