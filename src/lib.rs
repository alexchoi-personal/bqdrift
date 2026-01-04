pub mod bq_runner;
pub mod diff;
pub mod drift;
pub mod dsl;
pub mod error;
pub mod executor;
pub mod invariant;
pub mod migration;
pub mod repl;
pub mod schema;

pub use diff::{decode_sql, encode_sql, format_sql_diff, has_changes};
pub use drift::{
    compress_to_base64, decompress_from_base64, AuditTableRow, Checksums, DriftDetector,
    DriftReport, DriftState, ExecutionArtifact, ExecutionStatus, ImmutabilityChecker,
    ImmutabilityReport, ImmutabilityViolation, PartitionDrift, PartitionState, SourceAuditEntry,
    SourceAuditReport, SourceAuditor, SourceStatus,
};
pub use dsl::{
    QueryDef, QueryLoader, QueryValidator, ResolvedRevision, Revision, SqlDependencies,
    ValidationResult, VersionDef,
};
pub use error::{BqDriftError, Result};
pub use executor::{
    create_bigquery_executor, create_mock_executor, ColumnDef, ColumnInfo, Executor, ExecutorMode,
    ExecutorRunner, QueryResult,
};
pub use executor::{BqClient, PartitionWriter, Runner};
pub use invariant::{
    resolve_invariants_def, CheckResult, CheckStatus, InvariantCheck, InvariantChecker,
    InvariantDef, InvariantReport, InvariantsDef, InvariantsRef, Severity,
};
pub use migration::MigrationTracker;
pub use repl::{
    AsyncJsonRpcServer, InteractiveRepl, ReplCommand, ReplResult, ReplSession, ServerConfig,
    ServerConfigInfo, SessionInfo, SessionManager,
};
pub use schema::{
    BqType, ClusterConfig, Field, FieldMode, PartitionConfig, PartitionKey, PartitionType, Schema,
};
