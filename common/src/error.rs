use std::{error::Error, fmt};

pub struct ExitError(pub Box<dyn Error + Send + Sync>);

impl<E> From<E> for ExitError
where
    E: Into<Box<dyn Error + Send + Sync>>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

impl fmt::Debug for ExitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        let mut source = self.0.source();
        while let Some(s) = source {
            write!(f, "\nCaused by: {}", s)?;
            source = s.source();
        }

        Ok(())
    }
}
