use super::client::BqClient;
use super::invariant_runner::execute_with_invariants;
use crate::dsl::QueryDef;
use crate::error::{BqDriftError, Result};
use crate::invariant::InvariantReport;
use crate::schema::PartitionKey;

#[derive(Debug, Clone)]
pub struct PartitionWriteStats {
    pub query_name: String,
    pub version: u32,
    pub partition_key: PartitionKey,
    pub invariant_report: Option<InvariantReport>,
}

pub struct PartitionWriter {
    client: BqClient,
}

impl PartitionWriter {
    pub fn new(client: BqClient) -> Self {
        Self { client }
    }

    pub async fn write_partition(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
    ) -> Result<PartitionWriteStats> {
        self.write_partition_impl(query_def, partition_key, true)
            .await
    }

    pub async fn write_partition_skip_invariants(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
    ) -> Result<PartitionWriteStats> {
        self.write_partition_impl(query_def, partition_key, false)
            .await
    }

    async fn write_partition_impl(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
        run_invariants: bool,
    ) -> Result<PartitionWriteStats> {
        let partition_date = partition_key.to_naive_date();
        let version = query_def
            .get_version_for_date(partition_date)
            .ok_or_else(|| {
                BqDriftError::Partition(format!("No version found for partition {}", partition_key))
            })?;

        let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());
        let full_sql = Self::build_merge_sql(query_def, sql, &partition_key)?;

        let invariant_report = execute_with_invariants(
            &self.client,
            &query_def.destination,
            partition_date,
            version,
            run_invariants,
            || async { self.client.execute_query(&full_sql).await },
        )
        .await?;

        Ok(PartitionWriteStats {
            query_name: query_def.name.clone(),
            version: version.version,
            partition_key,
            invariant_report,
        })
    }

    fn build_merge_sql(
        query_def: &QueryDef,
        sql: &str,
        partition_key: &PartitionKey,
    ) -> Result<String> {
        let dest_table = format!(
            "{}.{}",
            query_def.destination.dataset, query_def.destination.table
        );
        let partition_field = query_def
            .destination
            .partition
            .field_name()
            .ok_or_else(|| {
                BqDriftError::Partition(format!(
                    "Partition field not specified for query '{}'",
                    query_def.name
                ))
            })?;
        Ok(super::sql_builder::build_merge_sql(
            &dest_table,
            partition_field,
            sql,
            partition_key,
        ))
    }

    pub async fn write_partition_truncate(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
    ) -> Result<PartitionWriteStats> {
        self.write_partition_truncate_impl(query_def, partition_key, true)
            .await
    }

    pub async fn write_partition_truncate_skip_invariants(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
    ) -> Result<PartitionWriteStats> {
        self.write_partition_truncate_impl(query_def, partition_key, false)
            .await
    }

    async fn write_partition_truncate_impl(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
        run_invariants: bool,
    ) -> Result<PartitionWriteStats> {
        let partition_date = partition_key.to_naive_date();
        let version = query_def
            .get_version_for_date(partition_date)
            .ok_or_else(|| {
                BqDriftError::Partition(format!("No version found for partition {}", partition_key))
            })?;

        let dest_table = format!(
            "{}.{}{}",
            query_def.destination.dataset,
            query_def.destination.table,
            partition_key.decorator()
        );

        let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());
        let parameterized_sql = sql.replace(
            "@partition_date",
            &format!("'{}'", partition_key.sql_value()),
        );

        let insert_sql = format!(
            r#"
            INSERT INTO `{dest_table}`
            {parameterized_sql}
            "#,
            dest_table = dest_table,
            parameterized_sql = parameterized_sql,
        );

        let delete_sql = format!("DELETE FROM `{}` WHERE TRUE", dest_table);

        let client = &self.client;
        let invariant_report = execute_with_invariants(
            client,
            &query_def.destination,
            partition_date,
            version,
            run_invariants,
            || async {
                client.execute_query(&delete_sql).await?;
                client.execute_query(&insert_sql).await
            },
        )
        .await?;

        Ok(PartitionWriteStats {
            query_name: query_def.name.clone(),
            version: version.version,
            partition_key,
            invariant_report,
        })
    }
}
