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

macro_rules! define_error_from {
    ($ext:ty, $var:ident) => {
        impl From<$ext> for Error {
            fn from(err: $ext) -> Self {
                Error::$var { inner: err }
            }
        }
    };
}

define_error_from!(std::io::Error, Io);
define_error_from!(std::num::ParseIntError, ParseInt);
define_error_from!(phil_core::error::Error, Phil);
