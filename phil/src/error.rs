use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{inner}")]
    Io {
        #[from]
        inner: std::io::Error,
    },

    #[error("{inner}")]
    ParseInt {
        #[from]
        inner: std::num::ParseIntError,
    },

    #[error("{inner}")]
    Phil {
        #[from]
        inner: phil_core::error::Error,
    },
}
