//! Schema introspection, diff, and DDL generation.
//!
//! Used by diff, drift, and snapshot commands. Introspection has both a
//! PostgreSQL implementation ([`introspect`]) and a MySQL implementation
//! ([`introspect_mysql`]); [`introspect_db`] dispatches based on engine.
//! [`diff`] is engine-agnostic — it consumes [`SchemaSnapshot`] regardless
//! of which engine produced it. [`generate_ddl`] / [`to_ddl`] currently
//! emit PostgreSQL-flavored DDL only; MySQL diff results can be rendered
//! as structured `SchemaDiff` JSON.

use std::collections::{HashMap, HashSet};

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::db::{quote_ident, DbClient};
use crate::dialect::DialectKind;
use crate::error::Result;
#[cfg(any(not(feature = "postgres"), not(feature = "mysql")))]
use crate::error::WaypointError;

/// Complete snapshot of a PostgreSQL schema.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SchemaSnapshot {
    /// All base tables in the schema.
    pub tables: Vec<TableDef>,
    /// All views (regular and materialized) in the schema.
    pub views: Vec<ViewDef>,
    /// All indexes in the schema.
    pub indexes: Vec<IndexDef>,
    /// All sequences in the schema.
    pub sequences: Vec<SequenceDef>,
    /// All functions and procedures in the schema.
    pub functions: Vec<FunctionDef>,
    /// All enum types in the schema.
    pub enums: Vec<EnumDef>,
    /// All table constraints in the schema.
    pub constraints: Vec<ConstraintDef>,
    /// All triggers in the schema.
    pub triggers: Vec<TriggerDef>,
    /// Names of installed extensions (excluding plpgsql).
    pub extensions: Vec<String>,
}

/// Definition of a database table.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TableDef {
    /// Schema the table belongs to.
    pub schema: String,
    /// Name of the table.
    pub name: String,
    /// Columns belonging to this table.
    pub columns: Vec<ColumnDef>,
}

/// Definition of a table column.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ColumnDef {
    /// Name of the column.
    pub name: String,
    /// SQL data type of the column.
    pub data_type: String,
    /// Whether the column allows NULL values.
    pub is_nullable: bool,
    /// Default value expression, if any.
    pub default: Option<String>,
    /// Position of the column within its table (1-based).
    pub ordinal_position: i32,
}

/// Definition of a database view.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ViewDef {
    /// Schema the view belongs to.
    pub schema: String,
    /// Name of the view.
    pub name: String,
    /// SQL definition body of the view.
    pub definition: String,
    /// Whether this is a materialized view.
    pub is_materialized: bool,
}

/// Definition of a database index.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct IndexDef {
    /// Schema the index belongs to.
    pub schema: String,
    /// Name of the index.
    pub name: String,
    /// Name of the table the index is built on.
    pub table_name: String,
    /// Full CREATE INDEX DDL statement.
    pub definition: String,
    /// Whether this is a unique index.
    pub is_unique: bool,
}

/// Definition of a database sequence.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SequenceDef {
    /// Schema the sequence belongs to.
    pub schema: String,
    /// Name of the sequence.
    pub name: String,
    /// Data type of the sequence (e.g. bigint).
    pub data_type: String,
}

/// Definition of a database function or procedure.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FunctionDef {
    /// Schema the function belongs to.
    pub schema: String,
    /// Name of the function.
    pub name: String,
    /// Function argument signature.
    pub arguments: String,
    /// Return type of the function.
    pub return_type: String,
    /// Implementation language (e.g. plpgsql, sql).
    pub language: String,
    /// Full function definition body.
    pub definition: String,
}

/// Definition of a PostgreSQL enum type.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EnumDef {
    /// Schema the enum belongs to.
    pub schema: String,
    /// Name of the enum type.
    pub name: String,
    /// Ordered list of enum label values.
    pub values: Vec<String>,
}

/// Definition of a table constraint.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConstraintDef {
    /// Schema the constraint belongs to.
    pub schema: String,
    /// Name of the table the constraint is on.
    pub table_name: String,
    /// Name of the constraint.
    pub name: String,
    /// Type of constraint (e.g. PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK).
    pub constraint_type: String,
    /// Full constraint definition expression.
    pub definition: String,
}

/// Definition of a database trigger.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TriggerDef {
    /// Schema the trigger belongs to.
    pub schema: String,
    /// Name of the table the trigger is attached to.
    pub table_name: String,
    /// Name of the trigger.
    pub name: String,
    /// Action statement executed by the trigger.
    pub definition: String,
}

/// Differences between two schema snapshots.
#[derive(Debug, Clone, Serialize)]
pub enum SchemaDiff {
    /// A table was added in the target schema.
    TableAdded(TableDef),
    /// A table was dropped from the target schema.
    TableDropped(String),
    /// A column was added to an existing table.
    ColumnAdded { table: String, column: ColumnDef },
    /// A column was dropped from an existing table.
    ColumnDropped { table: String, column: String },
    /// A column definition was altered in an existing table.
    ColumnAltered {
        table: String,
        column: String,
        from: ColumnDef,
        to: ColumnDef,
    },
    /// An index was added in the target schema.
    IndexAdded(IndexDef),
    /// An index was dropped from the target schema.
    IndexDropped(String),
    /// A view was added in the target schema.
    ViewAdded(ViewDef),
    /// A view was dropped from the target schema.
    ViewDropped(String),
    /// A view definition was altered.
    ViewAltered {
        name: String,
        from: String,
        to: String,
    },
    /// A sequence was added in the target schema.
    SequenceAdded(SequenceDef),
    /// A sequence was dropped from the target schema.
    SequenceDropped(String),
    /// A function was added in the target schema.
    FunctionAdded(FunctionDef),
    /// A function was dropped from the target schema.
    FunctionDropped(String),
    /// A function definition was altered.
    FunctionAltered { name: String },
    /// An enum type was added in the target schema.
    EnumAdded(EnumDef),
    /// An enum type was dropped from the target schema.
    EnumDropped(String),
    /// A constraint was added in the target schema.
    ConstraintAdded(ConstraintDef),
    /// A constraint was dropped from the target schema.
    ConstraintDropped { table: String, name: String },
    /// A trigger was added in the target schema.
    TriggerAdded(TriggerDef),
    /// A trigger was dropped from the target schema.
    TriggerDropped { table: String, name: String },
    /// A PostgreSQL extension was added.
    ExtensionAdded(String),
    /// A PostgreSQL extension was dropped.
    ExtensionDropped(String),
}

impl std::fmt::Display for SchemaDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaDiff::TableAdded(t) => write!(f, "+ TABLE {}", t.name),
            SchemaDiff::TableDropped(n) => write!(f, "- TABLE {}", n),
            SchemaDiff::ColumnAdded { table, column } => {
                write!(
                    f,
                    "+ COLUMN {}.{} ({})",
                    table, column.name, column.data_type
                )
            }
            SchemaDiff::ColumnDropped { table, column } => {
                write!(f, "- COLUMN {}.{}", table, column)
            }
            SchemaDiff::ColumnAltered { table, column, .. } => {
                write!(f, "~ COLUMN {}.{}", table, column)
            }
            SchemaDiff::IndexAdded(idx) => write!(f, "+ INDEX {}", idx.name),
            SchemaDiff::IndexDropped(n) => write!(f, "- INDEX {}", n),
            SchemaDiff::ViewAdded(v) => write!(f, "+ VIEW {}", v.name),
            SchemaDiff::ViewDropped(n) => write!(f, "- VIEW {}", n),
            SchemaDiff::ViewAltered { name, .. } => write!(f, "~ VIEW {}", name),
            SchemaDiff::SequenceAdded(s) => write!(f, "+ SEQUENCE {}", s.name),
            SchemaDiff::SequenceDropped(n) => write!(f, "- SEQUENCE {}", n),
            SchemaDiff::FunctionAdded(func) => write!(f, "+ FUNCTION {}", func.name),
            SchemaDiff::FunctionDropped(n) => write!(f, "- FUNCTION {}", n),
            SchemaDiff::FunctionAltered { name } => write!(f, "~ FUNCTION {}", name),
            SchemaDiff::EnumAdded(e) => write!(f, "+ TYPE {} (enum)", e.name),
            SchemaDiff::EnumDropped(n) => write!(f, "- TYPE {} (enum)", n),
            SchemaDiff::ConstraintAdded(c) => {
                write!(f, "+ CONSTRAINT {} ON {}", c.name, c.table_name)
            }
            SchemaDiff::ConstraintDropped { table, name } => {
                write!(f, "- CONSTRAINT {} ON {}", name, table)
            }
            SchemaDiff::TriggerAdded(t) => write!(f, "+ TRIGGER {} ON {}", t.name, t.table_name),
            SchemaDiff::TriggerDropped { table, name } => {
                write!(f, "- TRIGGER {} ON {}", name, table)
            }
            SchemaDiff::ExtensionAdded(n) => write!(f, "+ EXTENSION {}", n),
            SchemaDiff::ExtensionDropped(n) => write!(f, "- EXTENSION {}", n),
        }
    }
}

/// Introspect the current state of a schema (dialect-aware entry).
pub async fn introspect_db(client: &DbClient, schema: &str) -> Result<SchemaSnapshot> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => introspect(client.as_postgres()?, schema).await,
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => introspect_mysql(client, schema).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in".into(),
        )),
    }
}

/// Introspect the current state of a PostgreSQL schema.
#[cfg(feature = "postgres")]
pub async fn introspect(client: &Client, schema: &str) -> Result<SchemaSnapshot> {
    let (tables, views, indexes, sequences, functions, enums, constraints, triggers, extensions) =
        tokio::try_join!(
            introspect_tables(client, schema),
            introspect_views(client, schema),
            introspect_indexes(client, schema),
            introspect_sequences(client, schema),
            introspect_functions(client, schema),
            introspect_enums(client, schema),
            introspect_constraints(client, schema),
            introspect_triggers(client, schema),
            introspect_extensions(client),
        )?;

    Ok(SchemaSnapshot {
        tables,
        views,
        indexes,
        sequences,
        functions,
        enums,
        constraints,
        triggers,
        extensions,
    })
}

#[cfg(feature = "postgres")]
async fn introspect_tables(client: &Client, schema: &str) -> Result<Vec<TableDef>> {
    let rows = client
        .query(
            "SELECT t.table_name, c.column_name, c.data_type, c.is_nullable, c.column_default, c.ordinal_position
             FROM information_schema.tables t
             LEFT JOIN information_schema.columns c
               ON t.table_schema = c.table_schema AND t.table_name = c.table_name
             WHERE t.table_schema = $1 AND t.table_type = 'BASE TABLE'
             ORDER BY t.table_name, c.ordinal_position",
            &[&schema],
        )
        .await?;

    let mut tables: Vec<TableDef> = Vec::new();
    let mut current_table: Option<String> = None;
    let mut columns: Vec<ColumnDef> = Vec::new();

    for row in &rows {
        let table_name: String = row.get(0);
        let col_name: Option<String> = row.get(1);

        if current_table.as_ref() != Some(&table_name) {
            if let Some(prev_name) = current_table.take() {
                tables.push(TableDef {
                    schema: schema.to_string(),
                    name: prev_name,
                    columns: std::mem::take(&mut columns),
                });
            }
            current_table = Some(table_name.clone());
        }

        if let Some(name) = col_name {
            columns.push(ColumnDef {
                name,
                data_type: row.get(2),
                is_nullable: row.get::<_, String>(3) == "YES",
                default: row.get(4),
                ordinal_position: row.get(5),
            });
        }
    }

    // Don't forget the last table
    if let Some(name) = current_table {
        tables.push(TableDef {
            schema: schema.to_string(),
            name,
            columns,
        });
    }

    Ok(tables)
}

#[cfg(feature = "postgres")]
async fn introspect_views(client: &Client, schema: &str) -> Result<Vec<ViewDef>> {
    // Regular views
    let rows = client
        .query(
            "SELECT table_name, view_definition
             FROM information_schema.views
             WHERE table_schema = $1
             ORDER BY table_name",
            &[&schema],
        )
        .await?;

    let mut views: Vec<ViewDef> = rows
        .iter()
        .map(|r| ViewDef {
            schema: schema.to_string(),
            name: r.get(0),
            definition: r.get::<_, Option<String>>(1).unwrap_or_default(),
            is_materialized: false,
        })
        .collect();

    // Materialized views
    let mat_rows = client
        .query(
            "SELECT c.relname, pg_get_viewdef(c.oid)
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relkind = 'm'
             ORDER BY c.relname",
            &[&schema],
        )
        .await?;

    for r in &mat_rows {
        views.push(ViewDef {
            schema: schema.to_string(),
            name: r.get(0),
            definition: r.get::<_, Option<String>>(1).unwrap_or_default(),
            is_materialized: true,
        });
    }

    Ok(views)
}

#[cfg(feature = "postgres")]
async fn introspect_indexes(client: &Client, schema: &str) -> Result<Vec<IndexDef>> {
    let rows = client
        .query(
            "SELECT indexname, tablename, indexdef
             FROM pg_indexes
             WHERE schemaname = $1
             ORDER BY indexname",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let definition: String = r.get(2);
            IndexDef {
                schema: schema.to_string(),
                name: r.get(0),
                table_name: r.get(1),
                is_unique: definition.to_uppercase().contains("UNIQUE"),
                definition,
            }
        })
        .collect())
}

#[cfg(feature = "postgres")]
async fn introspect_sequences(client: &Client, schema: &str) -> Result<Vec<SequenceDef>> {
    let rows = client
        .query(
            "SELECT sequence_name, data_type
             FROM information_schema.sequences
             WHERE sequence_schema = $1
             ORDER BY sequence_name",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| SequenceDef {
            schema: schema.to_string(),
            name: r.get(0),
            data_type: r.get(1),
        })
        .collect())
}

#[cfg(feature = "postgres")]
async fn introspect_functions(client: &Client, schema: &str) -> Result<Vec<FunctionDef>> {
    let rows = client
        .query(
            "SELECT p.proname,
                    pg_get_function_arguments(p.oid),
                    pg_get_function_result(p.oid),
                    l.lanname,
                    pg_get_functiondef(p.oid)
             FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
             JOIN pg_language l ON l.oid = p.prolang
             WHERE n.nspname = $1
               AND p.prokind IN ('f', 'p')
             ORDER BY p.proname",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| FunctionDef {
            schema: schema.to_string(),
            name: r.get(0),
            arguments: r.get(1),
            return_type: r.get::<_, Option<String>>(2).unwrap_or_default(),
            language: r.get(3),
            definition: r.get::<_, Option<String>>(4).unwrap_or_default(),
        })
        .collect())
}

#[cfg(feature = "postgres")]
async fn introspect_enums(client: &Client, schema: &str) -> Result<Vec<EnumDef>> {
    let rows = client
        .query(
            "SELECT t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder)::text[]
             FROM pg_type t
             JOIN pg_enum e ON e.enumtypid = t.oid
             JOIN pg_namespace n ON n.oid = t.typnamespace
             WHERE n.nspname = $1
             GROUP BY t.typname
             ORDER BY t.typname",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| EnumDef {
            schema: schema.to_string(),
            name: r.get(0),
            values: r.get(1),
        })
        .collect())
}

#[cfg(feature = "postgres")]
async fn introspect_constraints(client: &Client, schema: &str) -> Result<Vec<ConstraintDef>> {
    let rows = client
        .query(
            "SELECT tc.table_name, tc.constraint_name, tc.constraint_type,
                    pg_get_constraintdef(c.oid)
             FROM information_schema.table_constraints tc
             JOIN pg_constraint c ON c.conname = tc.constraint_name
             JOIN pg_namespace n ON n.oid = c.connamespace
             WHERE tc.constraint_schema = $1 AND n.nspname = $1
             ORDER BY tc.table_name, tc.constraint_name",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| ConstraintDef {
            schema: schema.to_string(),
            table_name: r.get(0),
            name: r.get(1),
            constraint_type: r.get(2),
            definition: r.get::<_, Option<String>>(3).unwrap_or_default(),
        })
        .collect())
}

#[cfg(feature = "postgres")]
async fn introspect_triggers(client: &Client, schema: &str) -> Result<Vec<TriggerDef>> {
    let rows = client
        .query(
            "SELECT event_object_table, trigger_name, action_statement
             FROM information_schema.triggers
             WHERE trigger_schema = $1
             ORDER BY event_object_table, trigger_name",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| TriggerDef {
            schema: schema.to_string(),
            table_name: r.get(0),
            name: r.get(1),
            definition: r.get(2),
        })
        .collect())
}

#[cfg(feature = "postgres")]
async fn introspect_extensions(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname",
            &[],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Compare two schema snapshots and return the differences.
pub fn diff(before: &SchemaSnapshot, after: &SchemaSnapshot) -> Vec<SchemaDiff> {
    let mut diffs = Vec::new();

    // Build lookup maps for O(1) access

    // Tables - keyed by name, value is reference to TableDef
    let before_tables: HashMap<&str, &TableDef> =
        before.tables.iter().map(|t| (t.name.as_str(), t)).collect();
    let after_tables: HashMap<&str, &TableDef> =
        after.tables.iter().map(|t| (t.name.as_str(), t)).collect();

    // Views - keyed by name, value is reference to ViewDef
    let before_views: HashMap<&str, &ViewDef> =
        before.views.iter().map(|v| (v.name.as_str(), v)).collect();
    let after_views: HashMap<&str, &ViewDef> =
        after.views.iter().map(|v| (v.name.as_str(), v)).collect();

    // Indexes - existence check only, keyed by name
    let before_indexes: HashSet<&str> = before.indexes.iter().map(|i| i.name.as_str()).collect();
    let after_indexes: HashSet<&str> = after.indexes.iter().map(|i| i.name.as_str()).collect();

    // Sequences - existence check only, keyed by name
    let before_sequences: HashSet<&str> =
        before.sequences.iter().map(|s| s.name.as_str()).collect();
    let after_sequences: HashSet<&str> = after.sequences.iter().map(|s| s.name.as_str()).collect();

    // Functions - keyed by name, value is reference to FunctionDef
    let before_functions: HashMap<&str, &FunctionDef> = before
        .functions
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();
    let after_functions: HashMap<&str, &FunctionDef> = after
        .functions
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();

    // Enums - existence check only, keyed by name
    let before_enums: HashSet<&str> = before.enums.iter().map(|e| e.name.as_str()).collect();
    let after_enums: HashSet<&str> = after.enums.iter().map(|e| e.name.as_str()).collect();

    // Constraints - compound key (table_name, name)
    let before_constraints: HashSet<(&str, &str)> = before
        .constraints
        .iter()
        .map(|c| (c.table_name.as_str(), c.name.as_str()))
        .collect();
    let after_constraints: HashSet<(&str, &str)> = after
        .constraints
        .iter()
        .map(|c| (c.table_name.as_str(), c.name.as_str()))
        .collect();

    // Triggers - compound key (table_name, name)
    let before_triggers: HashSet<(&str, &str)> = before
        .triggers
        .iter()
        .map(|t| (t.table_name.as_str(), t.name.as_str()))
        .collect();
    let after_triggers: HashSet<(&str, &str)> = after
        .triggers
        .iter()
        .map(|t| (t.table_name.as_str(), t.name.as_str()))
        .collect();

    // Extensions - existence check only
    let before_extensions: HashSet<&str> = before.extensions.iter().map(|e| e.as_str()).collect();
    let after_extensions: HashSet<&str> = after.extensions.iter().map(|e| e.as_str()).collect();

    // Tables: check dropped/altered then added
    for bt in &before.tables {
        if let Some(at) = after_tables.get(bt.name.as_str()) {
            diff_columns(&mut diffs, &bt.name, &bt.columns, &at.columns);
        } else {
            diffs.push(SchemaDiff::TableDropped(bt.name.clone()));
        }
    }
    for at in &after.tables {
        if !before_tables.contains_key(at.name.as_str()) {
            diffs.push(SchemaDiff::TableAdded(at.clone()));
        }
    }

    // Views: check dropped/altered then added
    for bv in &before.views {
        if let Some(av) = after_views.get(bv.name.as_str()) {
            if bv.definition != av.definition {
                diffs.push(SchemaDiff::ViewAltered {
                    name: bv.name.clone(),
                    from: bv.definition.clone(),
                    to: av.definition.clone(),
                });
            }
        } else {
            diffs.push(SchemaDiff::ViewDropped(bv.name.clone()));
        }
    }
    for av in &after.views {
        if !before_views.contains_key(av.name.as_str()) {
            diffs.push(SchemaDiff::ViewAdded(av.clone()));
        }
    }

    // Indexes: check dropped then added
    for bi in &before.indexes {
        if !after_indexes.contains(bi.name.as_str()) {
            diffs.push(SchemaDiff::IndexDropped(bi.name.clone()));
        }
    }
    for ai in &after.indexes {
        if !before_indexes.contains(ai.name.as_str()) {
            diffs.push(SchemaDiff::IndexAdded(ai.clone()));
        }
    }

    // Sequences: check dropped then added
    for bs in &before.sequences {
        if !after_sequences.contains(bs.name.as_str()) {
            diffs.push(SchemaDiff::SequenceDropped(bs.name.clone()));
        }
    }
    for a_s in &after.sequences {
        if !before_sequences.contains(a_s.name.as_str()) {
            diffs.push(SchemaDiff::SequenceAdded(a_s.clone()));
        }
    }

    // Functions: check dropped/altered then added
    for bf in &before.functions {
        if let Some(af) = after_functions.get(bf.name.as_str()) {
            if bf.definition != af.definition {
                diffs.push(SchemaDiff::FunctionAltered {
                    name: bf.name.clone(),
                });
            }
        } else {
            diffs.push(SchemaDiff::FunctionDropped(bf.name.clone()));
        }
    }
    for af in &after.functions {
        if !before_functions.contains_key(af.name.as_str()) {
            diffs.push(SchemaDiff::FunctionAdded(af.clone()));
        }
    }

    // Enums: check dropped then added
    for be in &before.enums {
        if !after_enums.contains(be.name.as_str()) {
            diffs.push(SchemaDiff::EnumDropped(be.name.clone()));
        }
    }
    for ae in &after.enums {
        if !before_enums.contains(ae.name.as_str()) {
            diffs.push(SchemaDiff::EnumAdded(ae.clone()));
        }
    }

    // Constraints: check dropped then added
    for bc in &before.constraints {
        if !after_constraints.contains(&(bc.table_name.as_str(), bc.name.as_str())) {
            diffs.push(SchemaDiff::ConstraintDropped {
                table: bc.table_name.clone(),
                name: bc.name.clone(),
            });
        }
    }
    for ac in &after.constraints {
        if !before_constraints.contains(&(ac.table_name.as_str(), ac.name.as_str())) {
            diffs.push(SchemaDiff::ConstraintAdded(ac.clone()));
        }
    }

    // Triggers: check dropped then added
    for bt in &before.triggers {
        if !after_triggers.contains(&(bt.table_name.as_str(), bt.name.as_str())) {
            diffs.push(SchemaDiff::TriggerDropped {
                table: bt.table_name.clone(),
                name: bt.name.clone(),
            });
        }
    }
    for at in &after.triggers {
        if !before_triggers.contains(&(at.table_name.as_str(), at.name.as_str())) {
            diffs.push(SchemaDiff::TriggerAdded(at.clone()));
        }
    }

    // Extensions: check dropped then added
    for ext in &before.extensions {
        if !after_extensions.contains(ext.as_str()) {
            diffs.push(SchemaDiff::ExtensionDropped(ext.clone()));
        }
    }
    for ext in &after.extensions {
        if !before_extensions.contains(ext.as_str()) {
            diffs.push(SchemaDiff::ExtensionAdded(ext.clone()));
        }
    }

    diffs
}

fn diff_columns(
    diffs: &mut Vec<SchemaDiff>,
    table: &str,
    before: &[ColumnDef],
    after: &[ColumnDef],
) {
    let before_cols: HashMap<&str, &ColumnDef> =
        before.iter().map(|c| (c.name.as_str(), c)).collect();
    let after_cols: HashMap<&str, &ColumnDef> =
        after.iter().map(|c| (c.name.as_str(), c)).collect();

    for bc in before {
        if let Some(ac) = after_cols.get(bc.name.as_str()) {
            if bc != *ac {
                diffs.push(SchemaDiff::ColumnAltered {
                    table: table.to_string(),
                    column: bc.name.clone(),
                    from: bc.clone(),
                    to: (*ac).clone(),
                });
            }
        } else {
            diffs.push(SchemaDiff::ColumnDropped {
                table: table.to_string(),
                column: bc.name.clone(),
            });
        }
    }
    for ac in after {
        if !before_cols.contains_key(ac.name.as_str()) {
            diffs.push(SchemaDiff::ColumnAdded {
                table: table.to_string(),
                column: ac.clone(),
            });
        }
    }
}

/// Generate DDL statements from schema diffs.
pub fn generate_ddl(diffs: &[SchemaDiff]) -> String {
    let mut statements = Vec::new();

    for d in diffs {
        match d {
            SchemaDiff::TableAdded(t) => {
                let cols: Vec<String> = t
                    .columns
                    .iter()
                    .map(|c| {
                        let mut col = format!("    {} {}", quote_ident(&c.name), c.data_type);
                        if !c.is_nullable {
                            col.push_str(" NOT NULL");
                        }
                        if let Some(ref default) = c.default {
                            col.push_str(&format!(" DEFAULT {}", default));
                        }
                        col
                    })
                    .collect();
                statements.push(format!(
                    "CREATE TABLE {} (\n{}\n);",
                    quote_ident(&t.name),
                    cols.join(",\n")
                ));
            }
            SchemaDiff::TableDropped(name) => {
                statements.push(format!(
                    "DROP TABLE IF EXISTS {} CASCADE;",
                    quote_ident(name)
                ));
            }
            SchemaDiff::ColumnAdded { table, column } => {
                let mut stmt = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    quote_ident(table),
                    quote_ident(&column.name),
                    column.data_type
                );
                if !column.is_nullable {
                    stmt.push_str(" NOT NULL");
                }
                if let Some(ref default) = column.default {
                    stmt.push_str(&format!(" DEFAULT {}", default));
                }
                stmt.push(';');
                statements.push(stmt);
            }
            SchemaDiff::ColumnDropped { table, column } => {
                statements.push(format!(
                    "ALTER TABLE {} DROP COLUMN {};",
                    quote_ident(table),
                    quote_ident(column)
                ));
            }
            SchemaDiff::ColumnAltered {
                table, column, to, ..
            } => {
                statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
                    quote_ident(table),
                    quote_ident(column),
                    to.data_type
                ));
                if to.is_nullable {
                    statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;",
                        quote_ident(table),
                        quote_ident(column)
                    ));
                } else {
                    statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                        quote_ident(table),
                        quote_ident(column)
                    ));
                }
                match &to.default {
                    Some(default) => {
                        statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                            quote_ident(table),
                            quote_ident(column),
                            default
                        ));
                    }
                    None => {
                        statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                            quote_ident(table),
                            quote_ident(column)
                        ));
                    }
                }
            }
            SchemaDiff::IndexAdded(idx) => {
                statements.push(format!("{};", idx.definition));
            }
            SchemaDiff::IndexDropped(name) => {
                statements.push(format!("DROP INDEX IF EXISTS {};", quote_ident(name)));
            }
            SchemaDiff::ViewAdded(v) => {
                let keyword = if v.is_materialized {
                    "MATERIALIZED VIEW"
                } else {
                    "VIEW"
                };
                statements.push(format!(
                    "CREATE {} {} AS {};",
                    keyword,
                    quote_ident(&v.name),
                    v.definition.trim_end_matches(';').trim()
                ));
            }
            SchemaDiff::ViewDropped(name) => {
                statements.push(format!(
                    "DROP VIEW IF EXISTS {} CASCADE;",
                    quote_ident(name)
                ));
            }
            SchemaDiff::ViewAltered { name, to, .. } => {
                statements.push(format!(
                    "CREATE OR REPLACE VIEW {} AS {};",
                    quote_ident(name),
                    to.trim_end_matches(';').trim()
                ));
            }
            SchemaDiff::SequenceAdded(s) => {
                statements.push(format!("CREATE SEQUENCE {};", quote_ident(&s.name)));
            }
            SchemaDiff::SequenceDropped(name) => {
                statements.push(format!("DROP SEQUENCE IF EXISTS {};", quote_ident(name)));
            }
            SchemaDiff::FunctionAdded(func) => {
                statements.push(format!("{};", func.definition.trim_end_matches(';')));
            }
            SchemaDiff::FunctionDropped(name) => {
                statements.push(format!(
                    "DROP FUNCTION IF EXISTS {} CASCADE;",
                    quote_ident(name)
                ));
            }
            SchemaDiff::FunctionAltered { name } => {
                // For altered functions we'd need the full definition; leave a comment
                statements.push(format!(
                    "-- Function {} was altered; manual review needed",
                    name
                ));
            }
            SchemaDiff::EnumAdded(e) => {
                let values: Vec<String> = e.values.iter().map(|v| format!("'{}'", v)).collect();
                statements.push(format!(
                    "CREATE TYPE {} AS ENUM ({});",
                    quote_ident(&e.name),
                    values.join(", ")
                ));
            }
            SchemaDiff::EnumDropped(name) => {
                statements.push(format!(
                    "DROP TYPE IF EXISTS {} CASCADE;",
                    quote_ident(name)
                ));
            }
            SchemaDiff::ConstraintAdded(c) => {
                statements.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} {};",
                    quote_ident(&c.table_name),
                    quote_ident(&c.name),
                    c.definition
                ));
            }
            SchemaDiff::ConstraintDropped { table, name } => {
                statements.push(format!(
                    "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};",
                    quote_ident(table),
                    quote_ident(name)
                ));
            }
            SchemaDiff::TriggerAdded(t) => {
                statements.push(format!(
                    "-- Trigger {} on {} needs manual creation",
                    t.name, t.table_name
                ));
            }
            SchemaDiff::TriggerDropped { table, name } => {
                statements.push(format!(
                    "DROP TRIGGER IF EXISTS {} ON {};",
                    quote_ident(name),
                    quote_ident(table)
                ));
            }
            SchemaDiff::ExtensionAdded(name) => {
                statements.push(format!(
                    "CREATE EXTENSION IF NOT EXISTS {};",
                    quote_ident(name)
                ));
            }
            SchemaDiff::ExtensionDropped(name) => {
                statements.push(format!("DROP EXTENSION IF EXISTS {};", quote_ident(name)));
            }
        }
    }

    statements.join("\n\n")
}

/// Generate full DDL to recreate a schema from a snapshot.
pub fn to_ddl(snapshot: &SchemaSnapshot) -> String {
    let mut statements = Vec::new();

    // Extensions first
    for ext in &snapshot.extensions {
        statements.push(format!(
            "CREATE EXTENSION IF NOT EXISTS {};",
            quote_ident(ext)
        ));
    }

    // Enums before tables (types must exist for columns)
    for e in &snapshot.enums {
        let values: Vec<String> = e.values.iter().map(|v| format!("'{}'", v)).collect();
        statements.push(format!(
            "CREATE TYPE {} AS ENUM ({});",
            quote_ident(&e.name),
            values.join(", ")
        ));
    }

    // Sequences
    for s in &snapshot.sequences {
        statements.push(format!("CREATE SEQUENCE {};", quote_ident(&s.name)));
    }

    // Tables
    for t in &snapshot.tables {
        let cols: Vec<String> = t
            .columns
            .iter()
            .map(|c| {
                let mut col = format!("    {} {}", quote_ident(&c.name), c.data_type);
                if !c.is_nullable {
                    col.push_str(" NOT NULL");
                }
                if let Some(ref default) = c.default {
                    col.push_str(&format!(" DEFAULT {}", default));
                }
                col
            })
            .collect();
        statements.push(format!(
            "CREATE TABLE {} (\n{}\n);",
            quote_ident(&t.name),
            cols.join(",\n")
        ));
    }

    // Constraints
    for c in &snapshot.constraints {
        statements.push(format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {};",
            quote_ident(&c.table_name),
            quote_ident(&c.name),
            c.definition
        ));
    }

    // Indexes
    for idx in &snapshot.indexes {
        statements.push(format!("{};", idx.definition));
    }

    // Views
    for v in &snapshot.views {
        let keyword = if v.is_materialized {
            "MATERIALIZED VIEW"
        } else {
            "VIEW"
        };
        statements.push(format!(
            "CREATE {} {} AS {};",
            keyword,
            quote_ident(&v.name),
            v.definition.trim_end_matches(';').trim()
        ));
    }

    // Functions
    for func in &snapshot.functions {
        statements.push(format!("{};", func.definition.trim_end_matches(';')));
    }

    // Triggers
    for t in &snapshot.triggers {
        statements.push(format!(
            "-- Trigger {} on {}: {}",
            t.name, t.table_name, t.definition
        ));
    }

    statements.join("\n\n")
}

// ── MySQL schema introspection ───────────────────────────────────────────────
//
// Produces the same SchemaSnapshot shape as PG `introspect()` so `diff()`
// works on either dialect. Concepts that don't exist on MySQL (sequences,
// PG-style enums, extensions) come back as empty vectors. Materialized views
// don't exist on MySQL 8.0 so `is_materialized` is always false here.

#[cfg(feature = "mysql")]
pub async fn introspect_mysql(client: &DbClient, schema: &str) -> Result<SchemaSnapshot> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;

    // Tables + columns (one row per column).
    let column_rows: Vec<(String, String, String, String, Option<String>, i32)> = conn
        .exec(
            "SELECT t.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, \
                    c.COLUMN_DEFAULT, c.ORDINAL_POSITION \
             FROM information_schema.TABLES t \
             JOIN information_schema.COLUMNS c \
               ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME \
             WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE' \
             ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION",
            (schema,),
        )
        .await?;
    let mut table_map: HashMap<String, Vec<ColumnDef>> = HashMap::new();
    for (table, col, dtype, nullable, default, ord) in column_rows {
        table_map.entry(table).or_default().push(ColumnDef {
            name: col,
            data_type: dtype,
            is_nullable: nullable == "YES",
            default,
            ordinal_position: ord,
        });
    }
    let mut tables: Vec<TableDef> = table_map
        .into_iter()
        .map(|(name, columns)| TableDef {
            schema: schema.to_string(),
            name,
            columns,
        })
        .collect();
    tables.sort_by(|a, b| a.name.cmp(&b.name));

    // Views.
    let view_rows: Vec<(String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
            (schema,),
        )
        .await?;
    let views: Vec<ViewDef> = view_rows
        .into_iter()
        .map(|(name, def)| ViewDef {
            schema: schema.to_string(),
            name,
            definition: def,
            is_materialized: false,
        })
        .collect();

    // Indexes — group STATISTICS rows by (table, index_name). PRIMARY indexes
    // surface as primary-key constraints instead.
    let index_rows: Vec<(String, String, i32, String, i64)> = conn
        .exec(
            "SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE \
             FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND INDEX_NAME <> 'PRIMARY' \
             ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
            (schema,),
        )
        .await?;
    let mut idx_map: HashMap<(String, String), (Vec<String>, bool)> = HashMap::new();
    for (table, idx_name, _seq, col, non_unique) in index_rows {
        let entry = idx_map
            .entry((table, idx_name))
            .or_insert_with(|| (Vec::new(), non_unique == 0));
        entry.0.push(col);
    }
    let mut indexes: Vec<IndexDef> = idx_map
        .into_iter()
        .map(|((table, name), (cols, is_unique))| {
            let kw = if is_unique {
                "CREATE UNIQUE INDEX"
            } else {
                "CREATE INDEX"
            };
            let definition = format!(
                "{} `{}` ON `{}` ({})",
                kw,
                name,
                table,
                cols.iter()
                    .map(|c| format!("`{}`", c))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            IndexDef {
                schema: schema.to_string(),
                name,
                table_name: table,
                definition,
                is_unique,
            }
        })
        .collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));

    // Routines (procedures + functions). We store both via FunctionDef.
    let routine_rows: Vec<(String, String, String, String)> = conn
        .exec(
            "SELECT ROUTINE_NAME, \
                    COALESCE(DTD_IDENTIFIER, ''), \
                    COALESCE(EXTERNAL_LANGUAGE, ROUTINE_BODY), \
                    COALESCE(ROUTINE_DEFINITION, '') \
             FROM information_schema.ROUTINES \
             WHERE ROUTINE_SCHEMA = ? ORDER BY ROUTINE_NAME",
            (schema,),
        )
        .await?;
    let functions: Vec<FunctionDef> = routine_rows
        .into_iter()
        .map(|(name, return_type, language, definition)| FunctionDef {
            schema: schema.to_string(),
            name,
            arguments: String::new(),
            return_type,
            language,
            definition,
        })
        .collect();

    // Constraints — PK / UNIQUE / FK. Definition is left empty for the diff
    // shape; the constraint type + name is the structural signal.
    let constraint_rows: Vec<(String, String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE \
             FROM information_schema.TABLE_CONSTRAINTS \
             WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, CONSTRAINT_NAME",
            (schema,),
        )
        .await?;
    let constraints: Vec<ConstraintDef> = constraint_rows
        .into_iter()
        .map(|(table, name, ctype)| ConstraintDef {
            schema: schema.to_string(),
            table_name: table,
            name,
            constraint_type: ctype,
            definition: String::new(),
        })
        .collect();

    // Triggers.
    let trigger_rows: Vec<(String, String, String)> = conn
        .exec(
            "SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME, ACTION_STATEMENT \
             FROM information_schema.TRIGGERS \
             WHERE TRIGGER_SCHEMA = ? ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME",
            (schema,),
        )
        .await?;
    let triggers: Vec<TriggerDef> = trigger_rows
        .into_iter()
        .map(|(table_name, name, definition)| TriggerDef {
            schema: schema.to_string(),
            table_name,
            name,
            definition,
        })
        .collect();

    Ok(SchemaSnapshot {
        tables,
        views,
        indexes,
        sequences: Vec::new(),
        functions,
        enums: Vec::new(),
        constraints,
        triggers,
        extensions: Vec::new(),
    })
}
