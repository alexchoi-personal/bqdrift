use std::path::Path;
use thiserror::Error;
use tracing::warn;

#[derive(Error, Debug)]
pub enum BqRunnerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Execution error: {0}")]
    Execution(String),
}

pub type Result<T> = std::result::Result<T, BqRunnerError>;

#[derive(Debug, Clone)]
pub struct SqlFile {
    pub path: std::path::PathBuf,
    pub content: String,
}

pub struct FileLoader;

impl FileLoader {
    pub fn load_dir(path: impl AsRef<Path>, extension: &str) -> Result<Vec<SqlFile>> {
        let path = path.as_ref();
        let pattern = format!("{}/**/*.{}", path.display(), extension);
        let glob_iter =
            glob::glob(&pattern).map_err(|e| BqRunnerError::Execution(e.to_string()))?;
        let (lower, upper) = glob_iter.size_hint();
        let mut files = Vec::with_capacity(upper.unwrap_or(lower));
        let mut skipped_count = 0;

        for entry in glob_iter {
            let file_path = match entry {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "Failed to read glob entry");
                    skipped_count += 1;
                    continue;
                }
            };

            match std::fs::read_to_string(&file_path) {
                Ok(content) => files.push(SqlFile {
                    path: file_path,
                    content,
                }),
                Err(e) => {
                    warn!(path = %file_path.display(), error = %e, "Failed to read file");
                    skipped_count += 1;
                }
            }
        }

        if skipped_count > 0 {
            warn!(skipped = skipped_count, "Some files could not be loaded");
        }

        Ok(files)
    }

    pub fn load_file(path: impl AsRef<Path>) -> Result<SqlFile> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)?;
        Ok(SqlFile {
            path: path.to_path_buf(),
            content,
        })
    }
}

pub struct SqlLoader;

impl SqlLoader {
    pub fn load_dir(path: impl AsRef<Path>) -> Result<Vec<SqlFile>> {
        FileLoader::load_dir(path, "sql")
    }

    pub fn load_file(path: impl AsRef<Path>) -> Result<SqlFile> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)?;
        Ok(SqlFile {
            path: path.to_path_buf(),
            content,
        })
    }
}
