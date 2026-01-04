use super::parser::{ExtendedSchema, SchemaRef};
use crate::error::{BqDriftError, Result};
use crate::invariant::{ExtendedInvariants, InvariantDef, InvariantsDef, InvariantsRef};
use crate::schema::{Field, Schema};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use tracing::warn;

static VARIABLE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\$\{\{\s*versions\.(\d+)\.(\w+)\s*\}\}").expect("variable pattern regex is valid")
});

pub struct VariableResolver;

impl VariableResolver {
    pub fn new() -> Self {
        Self
    }

    pub fn resolve_schema(
        &self,
        schema_ref: &SchemaRef,
        resolved_versions: &HashMap<u32, Schema>,
    ) -> Result<Schema> {
        match schema_ref {
            SchemaRef::Inline(fields) => Ok(Schema::from_fields(fields.clone())),

            SchemaRef::Reference(ref_str) => {
                let version = self.extract_version_ref(ref_str)?;
                resolved_versions.get(&version).cloned().ok_or_else(|| {
                    BqDriftError::InvalidVersionRef(format!(
                        "Version {} not found or not yet resolved",
                        version
                    ))
                })
            }

            SchemaRef::Extended(ext) => self.resolve_extended_schema(ext, resolved_versions),
        }
    }

    fn resolve_extended_schema(
        &self,
        ext: &ExtendedSchema,
        resolved_versions: &HashMap<u32, Schema>,
    ) -> Result<Schema> {
        let base_version = self.extract_version_ref(&ext.base)?;
        let base_schema = resolved_versions.get(&base_version).ok_or_else(|| {
            BqDriftError::InvalidVersionRef(format!("Base version {} not found", base_version))
        })?;

        let mut fields: Vec<Field> = base_schema.fields.clone();

        // Remove fields
        for name in &ext.remove {
            fields.retain(|f| &f.name != name);
        }

        // Modify existing fields (replace by name)
        for modified in &ext.modify {
            if let Some(field) = fields.iter_mut().find(|f| f.name == modified.name) {
                *field = modified.clone();
            }
        }

        // Add new fields
        fields.extend(ext.add.clone());

        Ok(Schema::from_fields(fields))
    }

    fn extract_version_ref(&self, ref_str: &str) -> Result<u32> {
        let caps = VARIABLE_PATTERN
            .captures(ref_str)
            .ok_or_else(|| BqDriftError::InvalidVersionRef(ref_str.to_string()))?;
        caps.get(1)
            .ok_or_else(|| BqDriftError::InvalidVersionRef(ref_str.to_string()))?
            .as_str()
            .parse()
            .map_err(|_| BqDriftError::InvalidVersionRef(ref_str.to_string()))
    }

    pub fn resolve_sql_ref(
        &self,
        sql_ref: &str,
        resolved_sqls: &HashMap<u32, String>,
    ) -> Result<String> {
        let Some(caps) = VARIABLE_PATTERN.captures(sql_ref) else {
            return Ok(sql_ref.to_string());
        };

        let version: u32 = caps
            .get(1)
            .ok_or_else(|| BqDriftError::InvalidVersionRef(sql_ref.to_string()))?
            .as_str()
            .parse()
            .map_err(|_| BqDriftError::InvalidVersionRef(sql_ref.to_string()))?;

        let field = caps
            .get(2)
            .ok_or_else(|| BqDriftError::InvalidVersionRef(sql_ref.to_string()))?
            .as_str();
        if field != "sql" {
            return Err(BqDriftError::VariableResolution(format!(
                "Expected 'sql' field, got '{}'",
                field
            )));
        }

        resolved_sqls.get(&version).cloned().ok_or_else(|| {
            BqDriftError::InvalidVersionRef(format!("SQL for version {} not found", version))
        })
    }

    pub fn is_variable_ref(&self, s: &str) -> bool {
        VARIABLE_PATTERN.is_match(s)
    }

    pub fn resolve_invariants(
        &self,
        inv_ref: &Option<InvariantsRef>,
        resolved_versions: &HashMap<u32, InvariantsDef>,
    ) -> Result<InvariantsDef> {
        let result = match inv_ref {
            None => InvariantsDef::default(),

            Some(InvariantsRef::Inline(def)) => def.clone(),

            Some(InvariantsRef::Reference(ref_str)) => {
                let version = self.extract_invariants_version_ref(ref_str)?;
                resolved_versions.get(&version).cloned().ok_or_else(|| {
                    BqDriftError::InvalidVersionRef(format!(
                        "Invariants for version {} not found or not yet resolved",
                        version
                    ))
                })?
            }

            Some(InvariantsRef::Extended(ext)) => {
                self.resolve_extended_invariants(ext, resolved_versions)?
            }
        };

        self.validate_invariants_def(&result)?;
        Ok(result)
    }

    fn validate_invariants_def(&self, def: &InvariantsDef) -> Result<()> {
        for inv in &def.before {
            if let Err(msg) = inv.check.validate() {
                warn!(
                    invariant = %inv.name,
                    phase = "before",
                    error = %msg,
                    "Invalid invariant check configuration"
                );
                return Err(BqDriftError::Validation(format!(
                    "Invariant '{}' (before): {}",
                    inv.name, msg
                )));
            }
        }
        for inv in &def.after {
            if let Err(msg) = inv.check.validate() {
                warn!(
                    invariant = %inv.name,
                    phase = "after",
                    error = %msg,
                    "Invalid invariant check configuration"
                );
                return Err(BqDriftError::Validation(format!(
                    "Invariant '{}' (after): {}",
                    inv.name, msg
                )));
            }
        }
        Ok(())
    }

    fn resolve_extended_invariants(
        &self,
        ext: &ExtendedInvariants,
        resolved_versions: &HashMap<u32, InvariantsDef>,
    ) -> Result<InvariantsDef> {
        let base_version = self.extract_invariants_version_ref(&ext.base)?;
        let base = resolved_versions.get(&base_version).ok_or_else(|| {
            BqDriftError::InvalidVersionRef(format!(
                "Base invariants version {} not found",
                base_version
            ))
        })?;

        let mut before: Vec<InvariantDef> = base.before.clone();
        let mut after: Vec<InvariantDef> = base.after.clone();

        if let Some(remove) = &ext.remove {
            before.retain(|inv| !remove.before.contains(&inv.name));
            after.retain(|inv| !remove.after.contains(&inv.name));
        }

        if let Some(modify) = &ext.modify {
            for modified in &modify.before {
                if let Some(inv) = before.iter_mut().find(|i| i.name == modified.name) {
                    *inv = modified.clone();
                }
            }
            for modified in &modify.after {
                if let Some(inv) = after.iter_mut().find(|i| i.name == modified.name) {
                    *inv = modified.clone();
                }
            }
        }

        if let Some(add) = &ext.add {
            before.extend(add.before.clone());
            after.extend(add.after.clone());
        }

        Ok(InvariantsDef { before, after })
    }

    fn extract_invariants_version_ref(&self, ref_str: &str) -> Result<u32> {
        let caps = VARIABLE_PATTERN
            .captures(ref_str)
            .ok_or_else(|| BqDriftError::InvalidVersionRef(ref_str.to_string()))?;

        let version: u32 = caps
            .get(1)
            .ok_or_else(|| BqDriftError::InvalidVersionRef(ref_str.to_string()))?
            .as_str()
            .parse()
            .map_err(|_| BqDriftError::InvalidVersionRef(ref_str.to_string()))?;

        let field = caps
            .get(2)
            .ok_or_else(|| BqDriftError::InvalidVersionRef(ref_str.to_string()))?
            .as_str();
        if field != "invariants" {
            return Err(BqDriftError::VariableResolution(format!(
                "Expected 'invariants' field, got '{}'",
                field
            )));
        }

        Ok(version)
    }
}

impl Default for VariableResolver {
    fn default() -> Self {
        Self::new()
    }
}
