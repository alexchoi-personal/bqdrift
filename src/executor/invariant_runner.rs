use crate::dsl::{Destination, VersionDef};
use crate::error::{BqDriftError, Result};
use crate::invariant::{
    resolve_invariants_def, CheckStatus, InvariantChecker, InvariantReport, ResolvedInvariant,
    Severity,
};
use chrono::NaiveDate;
use std::future::Future;

use super::client::BqClient;

pub(crate) async fn run_before_checks(
    client: &BqClient,
    destination: &Destination,
    partition_date: NaiveDate,
    before_checks: &[ResolvedInvariant],
) -> Result<Vec<crate::invariant::CheckResult>> {
    if before_checks.is_empty() {
        return Ok(Vec::new());
    }

    let checker = InvariantChecker::new(client, destination, partition_date);
    let results = checker.run_checks(before_checks).await?;

    let has_error = results
        .iter()
        .any(|r| r.status == CheckStatus::Failed && r.severity == Severity::Error);

    if has_error {
        return Err(BqDriftError::InvariantFailed(
            "Before invariant check(s) failed with error severity".to_string(),
        ));
    }

    Ok(results)
}

pub(crate) async fn run_after_checks(
    client: &BqClient,
    destination: &Destination,
    partition_date: NaiveDate,
    after_checks: &[ResolvedInvariant],
) -> Result<Vec<crate::invariant::CheckResult>> {
    if after_checks.is_empty() {
        return Ok(Vec::new());
    }

    let checker = InvariantChecker::new(client, destination, partition_date);
    checker.run_checks(after_checks).await
}

pub(crate) async fn execute_with_invariants<F, Fut>(
    client: &BqClient,
    destination: &Destination,
    partition_date: NaiveDate,
    version: &VersionDef,
    run_invariants: bool,
    execute_fn: F,
) -> Result<Option<InvariantReport>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    if !run_invariants {
        execute_fn().await?;
        return Ok(None);
    }

    let (before_checks, after_checks) = resolve_invariants_def(&version.invariants);

    let before_results =
        run_before_checks(client, destination, partition_date, &before_checks).await?;

    execute_fn().await?;

    let after_results =
        run_after_checks(client, destination, partition_date, &after_checks).await?;

    Ok(Some(InvariantReport {
        before: before_results,
        after: after_results,
    }))
}
