use err_derive::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(display = "{}", inner)]
    Io {
        #[error(cause)]
        inner: std::io::Error,
    },

    #[error(display = "{}", inner)]
    ParseInt {
        #[error(cause)]
        inner: std::num::ParseIntError,
    },

    #[error(display = "{}", inner)]
    Phil {
        #[error(cause)]
        inner: phil_core::error::Error,
    },
}
