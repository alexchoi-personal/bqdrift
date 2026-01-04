use crate::schema::PartitionKey;

pub(crate) fn build_merge_sql(
    dest_table: &str,
    partition_field: &str,
    sql: &str,
    partition_key: &PartitionKey,
) -> String {
    let parameterized_sql = sql.replace(
        "@partition_date",
        &format!("'{}'", partition_key.sql_value()),
    );

    let partition_condition = match partition_key {
        PartitionKey::Hour(_) => format!(
            "TIMESTAMP_TRUNC(target.{}, HOUR) = {}",
            partition_field,
            partition_key.sql_literal()
        ),
        PartitionKey::Day(_) => format!(
            "target.{} = {}",
            partition_field,
            partition_key.sql_literal()
        ),
        PartitionKey::Month { .. } => format!(
            "DATE_TRUNC(target.{}, MONTH) = {}",
            partition_field,
            partition_key.sql_literal()
        ),
        PartitionKey::Year(_) => format!(
            "DATE_TRUNC(target.{}, YEAR) = {}",
            partition_field,
            partition_key.sql_literal()
        ),
        PartitionKey::Range(_) => format!(
            "target.{} = {}",
            partition_field,
            partition_key.sql_literal()
        ),
    };

    format!(
        r#"
            MERGE `{dest_table}` AS target
            USING (
                {parameterized_sql}
            ) AS source
            ON FALSE
            WHEN NOT MATCHED BY SOURCE AND {partition_condition} THEN DELETE
            WHEN NOT MATCHED BY TARGET THEN INSERT ROW
            "#,
        dest_table = dest_table,
        parameterized_sql = parameterized_sql,
        partition_condition = partition_condition,
    )
}
