use super::client::BqClient;
use super::partition_writer::{PartitionWriteStats, PartitionWriter};
use crate::dsl::QueryDef;
use crate::error::Result;
use crate::schema::PartitionKey;
use chrono::{NaiveDate, Utc};
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;

fn default_parallelism() -> usize {
    std::env::var("BQDRIFT_PARALLELISM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}

#[derive(Debug)]
pub struct RunReport {
    pub stats: Vec<PartitionWriteStats>,
    pub failures: Vec<RunFailure>,
}

#[derive(Debug)]
pub struct RunFailure {
    pub query_name: String,
    pub partition_key: PartitionKey,
    pub error: String,
}

pub struct Runner {
    writer: PartitionWriter,
    queries: Arc<Vec<QueryDef>>,
    query_index: HashMap<String, usize>,
    parallelism: usize,
}

impl Runner {
    pub fn new(client: BqClient, queries: Arc<Vec<QueryDef>>) -> Self {
        let query_index = queries
            .iter()
            .enumerate()
            .map(|(i, q)| (q.name.clone(), i))
            .collect();
        Self {
            writer: PartitionWriter::new(client),
            queries,
            query_index,
            parallelism: default_parallelism(),
        }
    }

    fn get_query(&self, name: &str) -> Option<&QueryDef> {
        self.query_index.get(name).map(|&i| &self.queries[i])
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism.max(1);
        self
    }

    pub async fn run_today(&self) -> Result<RunReport> {
        let today = Utc::now().date_naive();
        self.run_for_date(today).await
    }

    pub async fn run_for_date(&self, date: NaiveDate) -> Result<RunReport> {
        self.run_for_partition(PartitionKey::Day(date)).await
    }

    pub async fn run_for_partition(&self, partition_key: PartitionKey) -> Result<RunReport> {
        let results: Vec<_> = stream::iter(0..self.queries.len())
            .map(|idx| async move {
                let query = &self.queries[idx];
                let result = self.writer.write_partition(query, partition_key).await;
                (idx, result)
            })
            .buffer_unordered(self.parallelism)
            .collect()
            .await;

        let mut stats = Vec::new();
        let mut failures = Vec::new();

        for (idx, result) in results {
            match result {
                Ok(s) => stats.push(s),
                Err(e) => failures.push(RunFailure {
                    query_name: self.queries[idx].name.clone(),
                    partition_key,
                    error: e.to_string(),
                }),
            }
        }

        Ok(RunReport { stats, failures })
    }

    pub async fn run_query(
        &self,
        query_name: &str,
        date: NaiveDate,
    ) -> Result<PartitionWriteStats> {
        self.run_query_partition(query_name, PartitionKey::Day(date))
            .await
    }

    pub async fn run_query_partition(
        &self,
        query_name: &str,
        partition_key: PartitionKey,
    ) -> Result<PartitionWriteStats> {
        let query = self
            .get_query(query_name)
            .ok_or_else(|| crate::error::BqDriftError::QueryNotFound(query_name.to_string()))?;

        self.writer.write_partition(query, partition_key).await
    }

    pub async fn backfill(
        &self,
        query_name: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<RunReport> {
        self.backfill_partitions(
            query_name,
            PartitionKey::Day(from),
            PartitionKey::Day(to),
            None,
        )
        .await
    }

    pub async fn backfill_partitions(
        &self,
        query_name: &str,
        from: PartitionKey,
        to: PartitionKey,
        interval: Option<i64>,
    ) -> Result<RunReport> {
        let query = self
            .get_query(query_name)
            .ok_or_else(|| crate::error::BqDriftError::QueryNotFound(query_name.to_string()))?;

        let mut partitions = Vec::new();
        let mut current = from;
        while current <= to {
            partitions.push(current);
            current = match interval {
                Some(i) => current.next_by(i),
                None => current.next(),
            };
        }

        let results: Vec<_> = stream::iter(partitions)
            .map(|pk| async move {
                let result = self.writer.write_partition(query, pk).await;
                (pk, result)
            })
            .buffer_unordered(self.parallelism)
            .collect()
            .await;

        let mut stats = Vec::new();
        let mut failures = Vec::new();

        for (partition_key, result) in results {
            match result {
                Ok(s) => stats.push(s),
                Err(e) => failures.push(RunFailure {
                    query_name: query_name.to_string(),
                    partition_key,
                    error: e.to_string(),
                }),
            }
        }

        Ok(RunReport { stats, failures })
    }

    pub fn queries(&self) -> &[QueryDef] {
        &self.queries
    }
}
