use crate::error::Result;
use crate::executor::BqClient;
use chrono::{DateTime, NaiveDate, Utc};

const DEFAULT_TRACKING_TABLE: &str = "_bqdrift_query_runs";

fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

#[derive(Debug, Clone)]
pub struct QueryRun {
    pub query_name: String,
    pub query_version: u32,
    pub sql_revision: Option<u32>,
    pub partition_date: NaiveDate,
    pub executed_at: DateTime<Utc>,
    pub rows_written: Option<i64>,
    pub bytes_processed: Option<i64>,
    pub execution_time_ms: Option<i64>,
    pub status: RunStatus,
}

#[derive(Debug, Clone)]
pub enum RunStatus {
    Success,
    Failed,
}

pub struct MigrationTracker {
    client: BqClient,
    dataset: String,
    table_name: String,
}

impl MigrationTracker {
    pub fn new(client: BqClient, dataset: impl Into<String>) -> Self {
        Self {
            client,
            dataset: dataset.into(),
            table_name: DEFAULT_TRACKING_TABLE.to_string(),
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    fn full_table_name(&self) -> String {
        format!("{}.{}", self.dataset, self.table_name)
    }

    pub async fn ensure_tracking_table(&self) -> Result<()> {
        let table_name = self.full_table_name();

        let create_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                query_name STRING NOT NULL,
                query_version INT64 NOT NULL,
                sql_revision INT64,
                partition_date DATE NOT NULL,
                executed_at TIMESTAMP NOT NULL,
                rows_written INT64,
                bytes_processed INT64,
                execution_time_ms INT64,
                status STRING NOT NULL
            )
            PARTITION BY DATE(executed_at)
            "#,
            table_name = table_name
        );

        self.client.execute_query(&create_sql).await
    }

    pub async fn record_run(&self, run: &QueryRun) -> Result<()> {
        let table_name = self.full_table_name();
        let status_str = match run.status {
            RunStatus::Success => "SUCCESS",
            RunStatus::Failed => "FAILED",
        };

        let sql = format!(
            r#"
            INSERT INTO `{table_name}` (
                query_name, query_version, sql_revision, partition_date,
                executed_at, rows_written, bytes_processed, execution_time_ms, status
            ) VALUES (
                '{query_name}', {version}, {revision}, '{partition_date}',
                '{executed_at}', {rows}, {bytes}, {time_ms}, '{status}'
            )
            "#,
            table_name = table_name,
            query_name = escape_sql_string(&run.query_name),
            version = run.query_version,
            revision = run
                .sql_revision
                .map(|r| r.to_string())
                .unwrap_or("NULL".to_string()),
            partition_date = run.partition_date,
            executed_at =
                escape_sql_string(&run.executed_at.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
            rows = run
                .rows_written
                .map(|r| r.to_string())
                .unwrap_or("NULL".to_string()),
            bytes = run
                .bytes_processed
                .map(|b| b.to_string())
                .unwrap_or("NULL".to_string()),
            time_ms = run
                .execution_time_ms
                .map(|t| t.to_string())
                .unwrap_or("NULL".to_string()),
            status = status_str,
        );

        self.client.execute_query(&sql).await
    }

    #[deprecated(
        since = "0.2.0",
        note = "Not implemented. Use BigQuery directly to query the tracking table."
    )]
    pub async fn get_last_run(
        &self,
        _query_name: &str,
        _partition_date: NaiveDate,
    ) -> Result<Option<QueryRun>> {
        Ok(None)
    }

    #[deprecated(
        since = "0.2.0",
        note = "Not implemented. Use BigQuery directly to query the tracking table."
    )]
    pub async fn get_runs_for_date_range(
        &self,
        _query_name: &str,
        _from: NaiveDate,
        _to: NaiveDate,
    ) -> Result<Vec<QueryRun>> {
        Ok(Vec::new())
    }
}
