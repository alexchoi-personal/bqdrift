use super::dependencies::SqlDependencies;
use super::parser::{QueryDef, RawQueryDef, ResolvedRevision, VersionDef};
use super::preprocessor::YamlPreprocessor;
use super::resolver::VariableResolver;
use crate::bq_runner::{FileLoader, SqlFile, SqlLoader};
use crate::error::{BqDriftError, Result};
use crate::invariant::InvariantsDef;
use crate::schema::{ClusterConfig, Schema};
use std::collections::HashMap;
use std::path::Path;

pub struct QueryLoader {
    resolver: VariableResolver,
    preprocessor: YamlPreprocessor,
}

impl QueryLoader {
    pub fn new() -> Self {
        Self {
            resolver: VariableResolver::new(),
            preprocessor: YamlPreprocessor::new(),
        }
    }

    pub fn load_dir(&self, path: impl AsRef<Path>) -> Result<Vec<QueryDef>> {
        let (queries, _) = self.load_dir_with_contents(path)?;
        Ok(queries)
    }

    pub fn load_dir_with_contents(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(Vec<QueryDef>, HashMap<String, String>)> {
        let yaml_files = FileLoader::load_dir(&path, "yaml")
            .map_err(|e| BqDriftError::DslParse(e.to_string()))?;

        let mut queries = Vec::with_capacity(yaml_files.len());
        let mut contents = HashMap::with_capacity(yaml_files.len());

        for file in yaml_files {
            let base_dir = file.path.parent().unwrap_or(Path::new("."));
            let processed = self.preprocessor.process(&file.content, base_dir)?;
            let raw: RawQueryDef = serde_yaml::from_str(&processed)?;
            let name = raw.name.clone();
            let query = self.resolve_query(raw)?;
            queries.push(query);
            contents.insert(name, processed);
        }

        Ok((queries, contents))
    }

    pub fn load_sql_dir(&self, path: impl AsRef<Path>) -> Result<Vec<SqlFile>> {
        SqlLoader::load_dir(path).map_err(|e| BqDriftError::DslParse(e.to_string()))
    }

    pub fn load_sql_file(&self, path: impl AsRef<Path>) -> Result<SqlFile> {
        SqlLoader::load_file(path).map_err(|e| BqDriftError::DslParse(e.to_string()))
    }

    pub fn load_yaml_contents(&self, path: impl AsRef<Path>) -> Result<HashMap<String, String>> {
        let (_, contents) = self.load_dir_with_contents(path)?;
        Ok(contents)
    }

    pub fn load_query(&self, yaml_path: impl AsRef<Path>) -> Result<QueryDef> {
        let yaml_path = yaml_path.as_ref();
        let file =
            FileLoader::load_file(yaml_path).map_err(|e| BqDriftError::DslParse(e.to_string()))?;

        let base_dir = yaml_path.parent().unwrap_or(Path::new("."));
        let processed = self.preprocessor.process(&file.content, base_dir)?;

        let raw: RawQueryDef = serde_yaml::from_str(&processed)?;

        self.resolve_query(raw)
    }

    fn resolve_query(&self, mut raw: RawQueryDef) -> Result<QueryDef> {
        let version_count = raw.versions.len();
        let mut resolved_schemas: HashMap<u32, Schema> = HashMap::with_capacity(version_count);
        let mut resolved_invariants: HashMap<u32, InvariantsDef> =
            HashMap::with_capacity(version_count);
        let mut versions: Vec<VersionDef> = Vec::with_capacity(version_count);

        raw.versions.sort_by(|a, b| {
            a.effective_from
                .cmp(&b.effective_from)
                .then_with(|| a.version.cmp(&b.version))
        });

        for raw_version in raw.versions {
            let schema = self
                .resolver
                .resolve_schema(&raw_version.schema, &resolved_schemas)?;

            let dependencies = SqlDependencies::extract(&raw_version.source).tables;
            let sql_content = raw_version.source;

            let revisions = self.resolve_revisions(&raw_version.revisions)?;

            let invariants = self
                .resolver
                .resolve_invariants(&raw_version.invariants, &resolved_invariants)?;

            resolved_schemas.insert(raw_version.version, schema.clone());
            resolved_invariants.insert(raw_version.version, invariants.clone());

            versions.push(VersionDef {
                version: raw_version.version,
                effective_from: raw_version.effective_from,
                source: "<inline>".to_string(),
                sql_content,
                revisions,
                description: raw_version.description,
                backfill_since: raw_version.backfill_since,
                schema,
                dependencies,
                invariants,
            });
        }

        let cluster = match &raw.destination.cluster {
            Some(fields) => Some(ClusterConfig::new(fields.clone())?),
            None => None,
        };

        Ok(QueryDef {
            name: raw.name,
            destination: raw.destination,
            description: raw.description,
            owner: raw.owner,
            tags: raw.tags,
            versions,
            cluster,
        })
    }

    fn resolve_revisions(
        &self,
        revisions: &[super::parser::Revision],
    ) -> Result<Vec<ResolvedRevision>> {
        revisions
            .iter()
            .map(|rev| {
                let sql_content = rev.source.clone();
                let dependencies = SqlDependencies::extract(&sql_content).tables;

                Ok(ResolvedRevision {
                    revision: rev.revision,
                    effective_from: rev.effective_from,
                    source: "<inline>".to_string(),
                    sql_content,
                    reason: rev.reason.clone(),
                    backfill_since: rev.backfill_since,
                    dependencies,
                })
            })
            .collect()
    }
}

impl Default for QueryLoader {
    fn default() -> Self {
        Self::new()
    }
}
