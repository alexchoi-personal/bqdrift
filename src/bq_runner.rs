use std::path::Path;
use thiserror::Error;

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
        let files: Vec<SqlFile> = glob::glob(&pattern)
            .map_err(|e| BqRunnerError::Execution(e.to_string()))?
            .filter_map(|entry| entry.ok())
            .filter_map(|path| {
                std::fs::read_to_string(&path).ok().map(|content| SqlFile {
                    path: path.clone(),
                    content,
                })
            })
            .collect();
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExecutorMode {
    Mock,
    BigQuery,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: String,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub column_type: String,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<String>>,
}

pub struct Executor {
    mode: ExecutorMode,
}

impl Executor {
    pub fn mock() -> Result<Self> {
        Ok(Self {
            mode: ExecutorMode::Mock,
        })
    }

    pub async fn bigquery() -> Result<Self> {
        Ok(Self {
            mode: ExecutorMode::BigQuery,
        })
    }

    pub fn mode(&self) -> ExecutorMode {
        self.mode
    }

    pub async fn execute(&self, _sql: &str) -> Result<u64> {
        Ok(0)
    }

    pub async fn query(&self, _sql: &str) -> Result<QueryResult> {
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }
}
