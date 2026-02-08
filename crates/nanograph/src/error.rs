use thiserror::Error;

#[derive(Debug, Error)]
pub enum NanoError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("plan error: {0}")]
    Plan(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("lance error: {0}")]
    Lance(String),

    #[error("manifest error: {0}")]
    Manifest(String),
}

pub type Result<T> = std::result::Result<T, NanoError>;
