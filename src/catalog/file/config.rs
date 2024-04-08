use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Configuration for the FileCatalog
#[derive(Debug, Clone, PartialEq)]
pub struct FileCatalogConfig {
    path: PathBuf,
    format: FileFormat,
}

impl FileCatalogConfig {
    /// Create a new [`FileCatalogConfig`]
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let path = PathBuf::from(path.as_ref());
        Self {
            path,
            format: FileFormat::Yaml,
        }
    }

    /// Set the FileFormat for the underlying configuration file
    pub fn with_format(mut self, format: FileFormat) -> Self {
        self.format = format;
        self
    }

    /// Return a reference to the path in the local filesystem
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the file format of the configuration file
    pub fn format(&self) -> FileFormat {
        self.format
    }
}

/// The file format where the share configuration is stored.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FileFormat {
    /// Json file format
    Json,
    /// Yaml file format
    Yaml,
    /// Toml file format
    Toml,
}
