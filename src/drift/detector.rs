use super::checksum::Checksums;
use super::state::{DriftReport, DriftState, PartitionDrift, PartitionState};
use crate::dsl::QueryDef;
use crate::error::{BqDriftError, Result};
use crate::schema::PartitionKey;
use chrono::NaiveDate;
use rayon::prelude::*;
use std::collections::HashMap;

const MAX_DETECTION_DAYS: i64 = 365 * 10;

pub struct DriftDetector<'a> {
    queries: HashMap<&'a str, &'a QueryDef>,
    yaml_contents: &'a HashMap<String, String>,
}

impl<'a> DriftDetector<'a> {
    pub fn new(queries: &'a [QueryDef], yaml_contents: &'a HashMap<String, String>) -> Self {
        let queries = queries.iter().map(|q| (q.name.as_str(), q)).collect();
        Self {
            queries,
            yaml_contents,
        }
    }

    pub fn detect(
        &self,
        stored_states: &[PartitionState],
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<DriftReport> {
        let num_days = (to - from).num_days().max(0);
        if num_days > MAX_DETECTION_DAYS {
            return Err(BqDriftError::Partition(format!(
                "Date range too large: {} days exceeds maximum of {} days",
                num_days, MAX_DETECTION_DAYS
            )));
        }
        let num_days = num_days as usize + 1;
        let estimated_capacity = self.queries.len() * num_days;

        let stored_map: HashMap<(&str, NaiveDate), &PartitionState> = stored_states
            .iter()
            .map(|s| ((s.query_name.as_str(), s.partition_date), s))
            .collect();

        let partitions: Vec<PartitionDrift> = self
            .queries
            .par_iter()
            .flat_map(|(&query_name, &query)| {
                let yaml_content = self
                    .yaml_contents
                    .get(query_name)
                    .map(|s| s.as_str())
                    .unwrap_or("");

                let mut checksum_cache: HashMap<u32, Checksums> = HashMap::new();
                let mut results = Vec::with_capacity(num_days);

                let mut current = from;
                while current <= to {
                    let drift = self.detect_partition_cached(
                        query_name,
                        query,
                        current,
                        stored_map.get(&(query_name, current)),
                        yaml_content,
                        &mut checksum_cache,
                    );
                    results.push(drift);
                    match current.succ_opt() {
                        Some(next) => current = next,
                        None => break,
                    }
                }
                results
            })
            .collect();

        let mut report = DriftReport::with_capacity(estimated_capacity);
        for drift in partitions {
            report.add(drift);
        }

        Ok(report)
    }

    fn detect_partition_cached(
        &self,
        query_name: &str,
        query: &QueryDef,
        partition_date: NaiveDate,
        stored: Option<&&PartitionState>,
        yaml_content: &str,
        checksum_cache: &mut HashMap<u32, Checksums>,
    ) -> PartitionDrift {
        let version = query.get_version_for_date(partition_date);

        let (state, executed_version, caused_by) = match (version, stored) {
            (None, _) => (DriftState::NeverRun, None, None),

            (Some(_), None) => (DriftState::NeverRun, None, None),

            (Some(v), Some(stored)) => {
                if stored.status == super::state::ExecutionStatus::Failed {
                    (DriftState::Failed, Some(stored.version), None)
                } else {
                    let current_checksums = checksum_cache.entry(v.version).or_insert_with(|| {
                        Checksums::from_version(v, yaml_content, chrono::Utc::now().date_naive())
                    });

                    if current_checksums.schema != stored.schema_checksum {
                        (DriftState::SchemaChanged, Some(stored.version), None)
                    } else if current_checksums.sql != stored.sql_checksum {
                        (DriftState::SqlChanged, Some(stored.version), None)
                    } else if v.version != stored.version {
                        (DriftState::VersionUpgraded, Some(stored.version), None)
                    } else {
                        (DriftState::Current, Some(stored.version), None)
                    }
                }
            }
        };

        let executed_sql_b64 = stored.and_then(|s| s.executed_sql_b64.clone());

        let current_sql = if state.needs_rerun() {
            version.map(|v| {
                v.get_sql_for_date(chrono::Utc::now().date_naive())
                    .to_string()
            })
        } else {
            None
        };

        PartitionDrift {
            query_name: query_name.to_string(),
            partition_key: PartitionKey::Day(partition_date),
            state,
            current_version: version.map(|v| v.version).unwrap_or(0),
            executed_version,
            caused_by,
            executed_sql_b64,
            current_sql,
        }
    }

    /// Check if any upstream dependency was re-run after this partition
    /// Returns the name of the upstream query that changed, if any
    pub fn detect_upstream_changed(
        &self,
        _query: &QueryDef,
        stored: &PartitionState,
        all_states: &[PartitionState],
    ) -> Option<String> {
        let state_index = Self::build_state_index(all_states);
        self.detect_upstream_changed_indexed(stored, &state_index)
    }

    fn build_state_index(
        all_states: &[PartitionState],
    ) -> HashMap<(&str, NaiveDate), &PartitionState> {
        let mut index: HashMap<(&str, NaiveDate), &PartitionState> =
            HashMap::with_capacity(all_states.len());
        for state in all_states {
            let key = (state.query_name.as_str(), state.partition_date);
            match index.get(&key) {
                Some(existing) if existing.executed_at >= state.executed_at => {}
                _ => {
                    index.insert(key, state);
                }
            }
        }
        index
    }

    fn detect_upstream_changed_indexed(
        &self,
        stored: &PartitionState,
        state_index: &HashMap<(&str, NaiveDate), &PartitionState>,
    ) -> Option<String> {
        for (upstream_name, recorded_time) in &stored.upstream_states {
            if let Some(upstream) =
                state_index.get(&(upstream_name.as_str(), stored.partition_date))
            {
                if upstream.executed_at > *recorded_time {
                    return Some(upstream_name.clone());
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drift::checksum::{compress_to_base64, Checksums};
    use crate::dsl::{Destination, VersionDef};
    use crate::invariant::InvariantsDef;
    use crate::schema::{PartitionConfig, Schema};
    use chrono::{NaiveDate, Utc};
    use std::collections::HashSet;

    fn create_test_query(name: &str, sql_content: &str) -> QueryDef {
        QueryDef {
            name: name.to_string(),
            destination: Destination {
                dataset: "test_dataset".to_string(),
                table: "test_table".to_string(),
                partition: PartitionConfig::day("date"),
                cluster: None,
            },
            description: None,
            owner: None,
            tags: vec![],
            versions: vec![VersionDef {
                version: 1,
                effective_from: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                source: "test.sql".to_string(),
                sql_content: sql_content.to_string(),
                revisions: vec![],
                description: None,
                backfill_since: None,
                schema: Schema::default(),
                dependencies: HashSet::new(),
                invariants: InvariantsDef::default(),
            }],
            cluster: None,
        }
    }

    fn create_stored_state(
        query_name: &str,
        partition_date: NaiveDate,
        sql_content: &str,
        yaml_content: &str,
    ) -> PartitionState {
        let checksums = Checksums::compute(sql_content, &Schema::default(), yaml_content);
        PartitionState {
            query_name: query_name.to_string(),
            partition_date,
            version: 1,
            sql_revision: None,
            effective_from: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            sql_checksum: checksums.sql,
            schema_checksum: checksums.schema,
            yaml_checksum: checksums.yaml,
            executed_sql_b64: Some(compress_to_base64(sql_content)),
            upstream_states: HashMap::new(),
            executed_at: Utc::now(),
            execution_time_ms: Some(100),
            rows_written: Some(1000),
            bytes_processed: Some(10000),
            status: super::super::state::ExecutionStatus::Success,
        }
    }

    #[test]
    fn test_detect_never_run_has_current_sql() {
        let query = create_test_query("test_query", "SELECT * FROM source");
        let yaml_contents =
            HashMap::from([("test_query".to_string(), "name: test_query".to_string())]);
        let queries = vec![query];
        let detector = DriftDetector::new(&queries, &yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let report = detector.detect(&[], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::NeverRun);
        assert!(drift.current_sql.is_some());
        assert!(drift
            .current_sql
            .as_ref()
            .unwrap()
            .contains("SELECT * FROM source"));
        assert!(drift.executed_sql_b64.is_none());
    }

    #[test]
    fn test_detect_current_preserves_executed_sql() {
        let sql = "SELECT * FROM source";
        let yaml = "name: test_query";
        let query = create_test_query("test_query", sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let queries = vec![query];
        let detector = DriftDetector::new(&queries, &yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let stored = create_stored_state("test_query", date, sql, yaml);

        let report = detector.detect(&[stored], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::Current);
        assert!(drift.current_sql.is_none());
        assert!(drift.executed_sql_b64.is_some());
    }

    #[test]
    fn test_detect_sql_changed_has_both_sqls() {
        let old_sql = "SELECT user_id FROM users";
        let new_sql = "SELECT COALESCE(user_id, 'anon') FROM users";
        let yaml = "name: test_query";

        let query = create_test_query("test_query", new_sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let queries = vec![query];
        let detector = DriftDetector::new(&queries, &yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let stored = create_stored_state("test_query", date, old_sql, yaml);

        let report = detector.detect(&[stored], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::SqlChanged);
        assert!(drift.current_sql.is_some());
        assert!(drift.current_sql.as_ref().unwrap().contains("COALESCE"));
        assert!(drift.executed_sql_b64.is_some());

        let executed = crate::diff::decode_sql(drift.executed_sql_b64.as_ref().unwrap());
        assert!(executed.is_none()); // executed_sql_b64 uses gzip compression, not plain base64
    }

    #[test]
    fn test_detect_sql_changed_executed_sql_decompresses() {
        let old_sql = "SELECT user_id FROM users";
        let new_sql = "SELECT COALESCE(user_id, 'anon') FROM users";
        let yaml = "name: test_query";

        let query = create_test_query("test_query", new_sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let queries = vec![query];
        let detector = DriftDetector::new(&queries, &yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let stored = create_stored_state("test_query", date, old_sql, yaml);

        let report = detector.detect(&[stored], date, date).unwrap();

        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::SqlChanged);

        let executed =
            crate::drift::decompress_from_base64(drift.executed_sql_b64.as_ref().unwrap());
        assert!(executed.is_some());
        assert_eq!(executed.unwrap(), old_sql);
    }

    #[test]
    fn test_detect_failed_state_preserves_executed_sql() {
        let sql = "SELECT * FROM source";
        let yaml = "name: test_query";
        let query = create_test_query("test_query", sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let queries = vec![query];
        let detector = DriftDetector::new(&queries, &yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let mut stored = create_stored_state("test_query", date, sql, yaml);
        stored.status = super::super::state::ExecutionStatus::Failed;

        let report = detector.detect(&[stored], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::Failed);
        assert!(drift.executed_sql_b64.is_some());
    }

    #[test]
    fn test_detect_schema_changed_preserves_executed_sql() {
        let sql = "SELECT * FROM source";
        let yaml = "name: test_query";
        let query = create_test_query("test_query", sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let queries = vec![query];
        let detector = DriftDetector::new(&queries, &yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let mut stored = create_stored_state("test_query", date, sql, yaml);
        stored.schema_checksum = "different_checksum".to_string();

        let report = detector.detect(&[stored], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::SchemaChanged);
        assert!(drift.executed_sql_b64.is_some());
        assert!(drift.current_sql.is_some());
    }

    #[test]
    fn test_detect_multiple_dates() {
        let sql = "SELECT * FROM source";
        let yaml = "name: test_query";
        let query = create_test_query("test_query", sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let queries = vec![query];
        let detector = DriftDetector::new(&queries, &yaml_contents);

        let from = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();

        let report = detector.detect(&[], from, to).unwrap();

        assert_eq!(report.partitions.len(), 5);
        for drift in &report.partitions {
            assert_eq!(drift.state, DriftState::NeverRun);
            assert!(drift.current_sql.is_some());
        }
    }
}
