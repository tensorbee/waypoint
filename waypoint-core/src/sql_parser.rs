//! Lightweight regex-based DDL extraction from SQL content.
//!
//! Used by lint, changelog, and conflict detection features.

use std::sync::LazyLock;

use regex_lite::Regex;
use serde::Serialize;

/// A DDL operation extracted from SQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum DdlOperation {
    /// A CREATE TABLE statement.
    CreateTable {
        /// Name of the table being created.
        table: String,
        /// Whether the statement includes IF NOT EXISTS.
        if_not_exists: bool,
    },
    /// A DROP TABLE statement.
    DropTable {
        /// Name of the table being dropped.
        table: String,
    },
    /// An ALTER TABLE ... ADD COLUMN statement.
    AlterTableAddColumn {
        /// Name of the table being altered.
        table: String,
        /// Name of the column being added.
        column: String,
        /// Data type of the new column.
        data_type: String,
        /// Whether the column has a DEFAULT expression.
        ///
        /// Determined from the parsed column definition only — a `DEFAULT`
        /// appearing inside a `CHECK (...)` expression, a string literal, or
        /// a comment does not count.
        has_default: bool,
        /// Whether the column has a NOT NULL constraint.
        ///
        /// Determined from the parsed column definition only — a `NOT NULL`
        /// appearing inside a `CHECK (...)` expression, a string literal, or
        /// a comment does not count.
        is_not_null: bool,
        /// Whether the clause includes `IF NOT EXISTS`.
        if_not_exists: bool,
        /// The `DEFAULT` expression of this column, if any.
        default_expr: Option<String>,
    },
    /// An ALTER TABLE ... DROP COLUMN statement.
    AlterTableDropColumn {
        /// Name of the table being altered.
        table: String,
        /// Name of the column being dropped.
        column: String,
    },
    /// An ALTER TABLE ... ALTER COLUMN statement.
    AlterTableAlterColumn {
        /// Name of the table being altered.
        table: String,
        /// Name of the column being modified.
        column: String,
    },
    /// A CREATE INDEX statement.
    CreateIndex {
        /// Name of the index being created.
        name: String,
        /// Name of the table the index is on.
        table: String,
        /// Whether the index is created CONCURRENTLY.
        is_concurrent: bool,
        /// Whether this is a UNIQUE index.
        is_unique: bool,
    },
    /// A DROP INDEX statement.
    DropIndex {
        /// Name of the index being dropped.
        name: String,
    },
    /// A CREATE VIEW or CREATE MATERIALIZED VIEW statement.
    CreateView {
        /// Name of the view being created.
        name: String,
        /// Whether this is a materialized view.
        is_materialized: bool,
    },
    /// A DROP VIEW statement.
    DropView {
        /// Name of the view being dropped.
        name: String,
    },
    /// A CREATE FUNCTION statement.
    CreateFunction {
        /// Name of the function being created.
        name: String,
    },
    /// A DROP FUNCTION statement.
    DropFunction {
        /// Name of the function being dropped.
        name: String,
    },
    /// An ALTER TABLE ... ADD CONSTRAINT statement.
    AddConstraint {
        /// Name of the table the constraint is added to.
        table: String,
        /// Type of constraint (e.g. PRIMARY KEY, UNIQUE, FOREIGN KEY).
        constraint_type: String,
    },
    /// An ALTER TABLE ... DROP CONSTRAINT statement.
    DropConstraint {
        /// Name of the table the constraint is dropped from.
        table: String,
        /// Name of the constraint being dropped.
        name: String,
    },
    /// A CREATE TYPE ... AS ENUM statement.
    CreateEnum {
        /// Name of the enum type being created.
        name: String,
    },
    /// A TRUNCATE TABLE statement.
    TruncateTable {
        /// Name of the table being truncated.
        table: String,
    },
    /// Any other SQL statement that does not match known DDL patterns.
    Other {
        /// Truncated preview of the unrecognized statement.
        statement_preview: String,
    },
}

impl std::fmt::Display for DdlOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DdlOperation::CreateTable {
                table,
                if_not_exists,
            } => {
                if *if_not_exists {
                    write!(f, "CREATE TABLE IF NOT EXISTS {}", table)
                } else {
                    write!(f, "CREATE TABLE {}", table)
                }
            }
            DdlOperation::DropTable { table } => write!(f, "DROP TABLE {}", table),
            DdlOperation::AlterTableAddColumn {
                table,
                column,
                data_type,
                if_not_exists,
                ..
            } => {
                let ine = if *if_not_exists { "IF NOT EXISTS " } else { "" };
                write!(
                    f,
                    "ALTER TABLE {} ADD COLUMN {}{} {}",
                    table, ine, column, data_type
                )
            }
            DdlOperation::AlterTableDropColumn { table, column } => {
                write!(f, "ALTER TABLE {} DROP COLUMN {}", table, column)
            }
            DdlOperation::AlterTableAlterColumn { table, column } => {
                write!(f, "ALTER TABLE {} ALTER COLUMN {}", table, column)
            }
            DdlOperation::CreateIndex {
                name,
                table,
                is_unique,
                is_concurrent,
            } => {
                let unique = if *is_unique { "UNIQUE " } else { "" };
                let concurrent = if *is_concurrent { "CONCURRENTLY " } else { "" };
                write!(
                    f,
                    "CREATE {}{}INDEX {} ON {}",
                    unique, concurrent, name, table
                )
            }
            DdlOperation::DropIndex { name } => write!(f, "DROP INDEX {}", name),
            DdlOperation::CreateView {
                name,
                is_materialized,
            } => {
                if *is_materialized {
                    write!(f, "CREATE MATERIALIZED VIEW {}", name)
                } else {
                    write!(f, "CREATE VIEW {}", name)
                }
            }
            DdlOperation::DropView { name } => write!(f, "DROP VIEW {}", name),
            DdlOperation::CreateFunction { name } => write!(f, "CREATE FUNCTION {}", name),
            DdlOperation::DropFunction { name } => write!(f, "DROP FUNCTION {}", name),
            DdlOperation::AddConstraint {
                table,
                constraint_type,
            } => {
                write!(
                    f,
                    "ALTER TABLE {} ADD {} CONSTRAINT",
                    table, constraint_type
                )
            }
            DdlOperation::DropConstraint { table, name } => {
                write!(f, "ALTER TABLE {} DROP CONSTRAINT {}", table, name)
            }
            DdlOperation::CreateEnum { name } => write!(f, "CREATE TYPE {} AS ENUM", name),
            DdlOperation::TruncateTable { table } => write!(f, "TRUNCATE TABLE {}", table),
            DdlOperation::Other { statement_preview } => write!(f, "{}", statement_preview),
        }
    }
}

// Regex patterns for DDL extraction
static CREATE_TABLE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)").unwrap()
});

static DROP_TABLE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)").unwrap()
});

static ALTER_TABLE_DROP_COLUMN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+DROP\s+(?:COLUMN\s+)?(?:IF\s+EXISTS\s+)?(\w+)",
    )
    .unwrap()
});

static ALTER_TABLE_ALTER_COLUMN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+ALTER\s+(?:COLUMN\s+)?(\w+)").unwrap()
});

static CREATE_INDEX_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)CREATE\s+(UNIQUE\s+)?INDEX\s+(CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\s+(?:(\w+)\.)?(\w+)").unwrap()
});

static DROP_INDEX_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)")
        .unwrap()
});

static CREATE_VIEW_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(MATERIALIZED\s+)?VIEW\s+(?:(\w+)\.)?(\w+)")
        .unwrap()
});

static DROP_VIEW_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)DROP\s+(MATERIALIZED\s+)?VIEW\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)").unwrap()
});

static CREATE_FUNCTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:(\w+)\.)?(\w+)").unwrap()
});

static DROP_FUNCTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)DROP\s+FUNCTION\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)").unwrap()
});

static ADD_CONSTRAINT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+ADD\s+(?:CONSTRAINT\s+\w+\s+)?(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK|EXCLUDE)").unwrap()
});

static DROP_CONSTRAINT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+DROP\s+CONSTRAINT\s+(?:IF\s+EXISTS\s+)?(\w+)",
    )
    .unwrap()
});

static CREATE_ENUM_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)CREATE\s+TYPE\s+(?:(\w+)\.)?(\w+)\s+AS\s+ENUM").unwrap());

static TRUNCATE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)TRUNCATE\s+(?:TABLE\s+)?(?:(\w+)\.)?(\w+)").unwrap());

/// A DDL operation together with its position in the source SQL.
///
/// Offsets are byte offsets into the **original** SQL text (comments
/// included), so diagnostics can point at the statement that produced the
/// operation even though the analysis itself runs on a comment-stripped copy.
#[derive(Debug, Clone)]
pub struct LocatedDdl {
    /// The parsed operation.
    pub op: DdlOperation,
    /// Byte offset of the first character of the statement.
    pub start: usize,
    /// Byte offset just past the last character of the statement.
    pub end: usize,
    /// Byte offset of the token this operation is best anchored to: the
    /// column name for `ADD COLUMN`, otherwise the leading keyword.
    pub focus: usize,
}

/// Extract DDL operations from SQL content.
///
/// Comments are ignored: they can neither introduce nor suppress an
/// operation, and keywords inside them (`NOT NULL`, `DEFAULT`, ...) never
/// influence the parsed result.
pub fn extract_ddl_operations(sql: &str) -> Vec<DdlOperation> {
    extract_ddl_operations_located(sql)
        .into_iter()
        .map(|l| l.op)
        .collect()
}

/// Extract DDL operations along with their source positions.
pub fn extract_ddl_operations_located(sql: &str) -> Vec<LocatedDdl> {
    let stripped = strip_comments(sql);
    let mut ops = Vec::new();

    for (start, end) in statement_ranges(&stripped) {
        let stmt = &stripped[start..end];
        let parsed = parse_statement_ops(stmt);
        if parsed.is_empty() {
            // Unrecognized statement — preview the original text so comments
            // written inside the statement still show up verbatim.
            let raw = &sql[start..end];
            let preview: String = raw.chars().take(80).collect();
            let preview = if raw.len() > 80 {
                format!("{}...", preview)
            } else {
                preview
            };
            ops.push(LocatedDdl {
                op: DdlOperation::Other {
                    statement_preview: preview,
                },
                start,
                end,
                focus: start,
            });
            continue;
        }
        for (op, offset) in parsed {
            ops.push(LocatedDdl {
                op,
                start,
                end,
                focus: start + offset,
            });
        }
    }

    ops
}

/// Parse a single comment-free statement into zero or more DDL operations.
///
/// Each operation is paired with the byte offset (relative to `stmt`) of the
/// token it should be reported against.
fn parse_statement_ops(stmt: &str) -> Vec<(DdlOperation, usize)> {
    // Order matters — more specific patterns first

    // ALTER TABLE ... ADD CONSTRAINT (before ADD COLUMN)
    if let Some(caps) = ADD_CONSTRAINT_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        let constraint_type = caps.get(3).unwrap().as_str().to_uppercase();
        return vec![(
            DdlOperation::AddConstraint {
                table,
                constraint_type,
            },
            caps.get(0).unwrap().start(),
        )];
    }

    // ALTER TABLE ... DROP CONSTRAINT (before DROP COLUMN)
    if let Some(caps) = DROP_CONSTRAINT_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        let name = caps.get(3).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::DropConstraint { table, name },
            caps.get(0).unwrap().start(),
        )];
    }

    // ALTER TABLE ... ALTER COLUMN (before ADD/DROP COLUMN)
    if let Some(caps) = ALTER_TABLE_ALTER_COLUMN_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        let column = caps.get(3).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::AlterTableAlterColumn { table, column },
            caps.get(0).unwrap().start(),
        )];
    }

    // ALTER TABLE ... DROP COLUMN
    if let Some(caps) = ALTER_TABLE_DROP_COLUMN_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        let column = caps.get(3).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::AlterTableDropColumn { table, column },
            caps.get(0).unwrap().start(),
        )];
    }

    // ALTER TABLE ... ADD [COLUMN] [IF NOT EXISTS] <column> <type> [constraints]
    if let Some((table, clauses)) = parse_add_columns(stmt) {
        return clauses
            .into_iter()
            .map(|c| {
                (
                    DdlOperation::AlterTableAddColumn {
                        table: table.clone(),
                        column: c.column,
                        data_type: c.data_type,
                        has_default: c.has_default,
                        is_not_null: c.is_not_null,
                        if_not_exists: c.if_not_exists,
                        default_expr: c.default_expr,
                    },
                    c.column_offset,
                )
            })
            .collect();
    }

    // CREATE TABLE
    if let Some(caps) = CREATE_TABLE_RE.captures(stmt) {
        let if_not_exists = caps.get(1).is_some();
        let table = caps.get(3).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::CreateTable {
                table,
                if_not_exists,
            },
            caps.get(0).unwrap().start(),
        )];
    }

    // DROP TABLE
    if let Some(caps) = DROP_TABLE_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::DropTable { table },
            caps.get(0).unwrap().start(),
        )];
    }

    // CREATE INDEX
    if let Some(caps) = CREATE_INDEX_RE.captures(stmt) {
        let is_unique = caps.get(1).is_some();
        let is_concurrent = caps.get(2).is_some();
        let name = caps.get(3).unwrap().as_str().to_string();
        let table = caps.get(5).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::CreateIndex {
                name,
                table,
                is_concurrent,
                is_unique,
            },
            caps.get(0).unwrap().start(),
        )];
    }

    // DROP INDEX
    if let Some(caps) = DROP_INDEX_RE.captures(stmt) {
        let name = caps.get(2).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::DropIndex { name },
            caps.get(0).unwrap().start(),
        )];
    }

    // CREATE [MATERIALIZED] VIEW
    if let Some(caps) = CREATE_VIEW_RE.captures(stmt) {
        let is_materialized = caps.get(1).is_some();
        let name = caps.get(3).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::CreateView {
                name,
                is_materialized,
            },
            caps.get(0).unwrap().start(),
        )];
    }

    // DROP VIEW
    if let Some(caps) = DROP_VIEW_RE.captures(stmt) {
        let name = caps.get(3).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::DropView { name },
            caps.get(0).unwrap().start(),
        )];
    }

    // CREATE FUNCTION
    if let Some(caps) = CREATE_FUNCTION_RE.captures(stmt) {
        let name = caps.get(2).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::CreateFunction { name },
            caps.get(0).unwrap().start(),
        )];
    }

    // DROP FUNCTION
    if let Some(caps) = DROP_FUNCTION_RE.captures(stmt) {
        let name = caps.get(2).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::DropFunction { name },
            caps.get(0).unwrap().start(),
        )];
    }

    // CREATE TYPE ... AS ENUM
    if let Some(caps) = CREATE_ENUM_RE.captures(stmt) {
        let name = caps.get(2).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::CreateEnum { name },
            caps.get(0).unwrap().start(),
        )];
    }

    // TRUNCATE
    if let Some(caps) = TRUNCATE_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        return vec![(
            DdlOperation::TruncateTable { table },
            caps.get(0).unwrap().start(),
        )];
    }

    Vec::new()
}

// ---------------------------------------------------------------------------
// ALTER TABLE ... ADD COLUMN parsing
// ---------------------------------------------------------------------------

/// A single `ADD [COLUMN] ...` clause parsed out of an `ALTER TABLE`.
#[derive(Debug, Clone)]
struct AddColumnClause {
    column: String,
    /// Byte offset of the column-name token within the statement.
    column_offset: usize,
    data_type: String,
    if_not_exists: bool,
    is_not_null: bool,
    has_default: bool,
    default_expr: Option<String>,
}

/// Keywords that terminate the data type and begin the constraint list of a
/// column definition. `CHARACTER` is deliberately absent — `CHARACTER
/// VARYING` and MySQL's `CHARACTER SET ...` both belong to the type.
const COLUMN_CONSTRAINT_KEYWORDS: &[&str] = &[
    "NOT",
    "NULL",
    "DEFAULT",
    "CHECK",
    "UNIQUE",
    "PRIMARY",
    "REFERENCES",
    "CONSTRAINT",
    "GENERATED",
    "COLLATE",
    "DEFERRABLE",
    "INITIALLY",
    "COMMENT",
    "AUTO_INCREMENT",
    "IDENTITY",
    "STORAGE",
    "COMPRESSION",
    "VISIBLE",
    "INVISIBLE",
    "FIRST",
    "AFTER",
];

/// Keywords that mean the token after `ADD` starts a table constraint rather
/// than a column definition.
const TABLE_CONSTRAINT_KEYWORDS: &[&str] = &[
    "CONSTRAINT",
    "PRIMARY",
    "UNIQUE",
    "FOREIGN",
    "CHECK",
    "EXCLUDE",
    "INDEX",
    "KEY",
    "FULLTEXT",
    "SPATIAL",
];

/// Parse every `ADD [COLUMN]` clause of an `ALTER TABLE` statement.
///
/// Handles the optional `COLUMN` keyword, the optional `IF NOT EXISTS`
/// clause, schema-qualified and quoted identifiers, parenthesised types, and
/// comma-separated clauses. `NOT NULL` and `DEFAULT` are only recognised at
/// the top level of the column definition, so they are never picked up from
/// inside a `CHECK (...)` expression or a string literal.
///
/// Returns `None` when the statement is not an `ALTER TABLE ... ADD <column>`.
fn parse_add_columns(stmt: &str) -> Option<(String, Vec<AddColumnClause>)> {
    let toks = tokenize(stmt);

    // Locate `ALTER TABLE`.
    let mut i = toks
        .windows(2)
        .position(|w| is_kw(&w[0], "ALTER") && is_kw(&w[1], "TABLE"))?
        + 2;

    // Optional PostgreSQL `ONLY` / `IF EXISTS` decorations.
    if kw_at(&toks, i, "IF") && kw_at(&toks, i + 1, "EXISTS") {
        i += 2;
    }
    if kw_at(&toks, i, "ONLY") {
        i += 1;
    }

    // Qualified table name: ident ('.' ident)*
    let mut name_parts: Vec<&str> = Vec::new();
    loop {
        let t = toks.get(i)?;
        if !is_identifier(t) {
            return None;
        }
        name_parts.push(t.text);
        i += 1;
        match toks.get(i) {
            Some(t) if t.kind == TokKind::Punct && t.text == "." => i += 1,
            _ => break,
        }
    }
    // PostgreSQL legacy inheritance marker: `ALTER TABLE parent * ...`
    if toks
        .get(i)
        .is_some_and(|t| t.kind == TokKind::Punct && t.text == "*")
    {
        i += 1;
    }
    let table = (*name_parts.last()?).to_string();

    // Walk the action list, picking up every top-level `ADD` clause.
    let mut clauses = Vec::new();
    let mut depth = 0usize;
    while i < toks.len() {
        let t = &toks[i];
        if t.kind == TokKind::Punct {
            match t.text {
                "(" => depth += 1,
                ")" => depth = depth.saturating_sub(1),
                _ => {}
            }
            i += 1;
            continue;
        }
        if depth == 0 && is_kw(t, "ADD") {
            if let Some((clause, next)) = parse_one_add_column(stmt, &toks, i + 1) {
                clauses.push(clause);
                i = next;
                continue;
            }
        }
        i += 1;
    }

    if clauses.is_empty() {
        None
    } else {
        Some((table, clauses))
    }
}

/// Parse one `ADD [COLUMN] [IF NOT EXISTS] <column> <type> [constraints]`
/// clause starting at token index `i` (the token just after `ADD`).
///
/// Returns the clause plus the index of the first token after it.
fn parse_one_add_column<'a>(
    stmt: &'a str,
    toks: &[Tok<'a>],
    mut i: usize,
) -> Option<(AddColumnClause, usize)> {
    if kw_at(toks, i, "COLUMN") {
        i += 1;
    }

    let mut if_not_exists = false;
    if kw_at(toks, i, "IF") && kw_at(toks, i + 1, "NOT") && kw_at(toks, i + 2, "EXISTS") {
        if_not_exists = true;
        i += 3;
    }

    let col = toks.get(i)?;
    if !is_identifier(col) {
        return None;
    }
    // `ADD CONSTRAINT ...`, `ADD PRIMARY KEY ...` etc. are not columns.
    if col.kind == TokKind::Word
        && TABLE_CONSTRAINT_KEYWORDS
            .iter()
            .any(|k| col.text.eq_ignore_ascii_case(k))
    {
        return None;
    }
    let column = col.text.to_string();
    let column_offset = col.start;
    i += 1;

    // Data type: everything up to the first top-level constraint keyword,
    // clause-terminating comma, or end of statement.
    let type_first = i;
    let mut depth = 0usize;
    while i < toks.len() {
        let t = &toks[i];
        if t.kind == TokKind::Punct {
            match t.text {
                "(" => depth += 1,
                ")" => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                "," if depth == 0 => break,
                _ => {}
            }
            i += 1;
            continue;
        }
        if depth == 0
            && t.kind == TokKind::Word
            && COLUMN_CONSTRAINT_KEYWORDS
                .iter()
                .any(|k| t.text.eq_ignore_ascii_case(k))
        {
            break;
        }
        i += 1;
    }
    let data_type = if i > type_first {
        normalize_whitespace(&stmt[toks[type_first].start..toks[i - 1].end])
    } else {
        "unknown".to_string()
    };

    // Constraint list: only top-level tokens count.
    let mut is_not_null = false;
    let mut default_expr = None;
    let mut depth = 0usize;
    while i < toks.len() {
        let t = &toks[i];
        if t.kind == TokKind::Punct {
            match t.text {
                "(" => depth += 1,
                ")" => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                "," if depth == 0 => {
                    i += 1;
                    break;
                }
                _ => {}
            }
            i += 1;
            continue;
        }
        if depth == 0 && t.kind == TokKind::Word {
            if is_kw(t, "NOT") && kw_at(toks, i + 1, "NULL") {
                is_not_null = true;
                i += 2;
                continue;
            }
            if is_kw(t, "DEFAULT") {
                let (expr, next) = read_default_expr(stmt, toks, i + 1);
                default_expr = Some(expr);
                i = next;
                continue;
            }
        }
        i += 1;
    }

    Some((
        AddColumnClause {
            column,
            column_offset,
            data_type,
            if_not_exists,
            is_not_null,
            has_default: default_expr.is_some(),
            default_expr,
        },
        i,
    ))
}

/// Read the expression following a top-level `DEFAULT`, stopping at the next
/// constraint keyword or the end of the column definition.
///
/// Returns the expression text and the index of the token after it. The first
/// token is always consumed so that `DEFAULT NULL` keeps its value.
fn read_default_expr(stmt: &str, toks: &[Tok<'_>], start: usize) -> (String, usize) {
    let mut i = start;
    let mut depth = 0usize;
    while i < toks.len() {
        let t = &toks[i];
        if t.kind == TokKind::Punct {
            match t.text {
                "(" => depth += 1,
                ")" => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                "," if depth == 0 => break,
                _ => {}
            }
            i += 1;
            continue;
        }
        if depth == 0
            && i > start
            && t.kind == TokKind::Word
            && COLUMN_CONSTRAINT_KEYWORDS
                .iter()
                .any(|k| t.text.eq_ignore_ascii_case(k))
        {
            break;
        }
        i += 1;
    }

    let expr = if i > start {
        normalize_whitespace(&stmt[toks[start].start..toks[i - 1].end])
    } else {
        String::new()
    };
    (expr, i)
}

/// Collapse runs of whitespace (including newlines) into single spaces.
fn normalize_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokKind {
    /// A bare word: keyword, unquoted identifier, or number.
    Word,
    /// A quoted identifier (`"col"` or `` `col` ``); `text` excludes the quotes.
    Ident,
    /// A string literal; `text` includes the quotes.
    Literal,
    /// A single punctuation character.
    Punct,
}

#[derive(Debug, Clone, Copy)]
struct Tok<'a> {
    kind: TokKind,
    text: &'a str,
    start: usize,
    end: usize,
}

fn is_kw(tok: &Tok<'_>, kw: &str) -> bool {
    tok.kind == TokKind::Word && tok.text.eq_ignore_ascii_case(kw)
}

fn kw_at(toks: &[Tok<'_>], i: usize, kw: &str) -> bool {
    toks.get(i).is_some_and(|t| is_kw(t, kw))
}

/// Whether a token can stand in for an identifier (bare word or quoted).
fn is_identifier(tok: &Tok<'_>) -> bool {
    match tok.kind {
        TokKind::Ident => true,
        TokKind::Word => tok
            .text
            .starts_with(|c: char| c.is_alphabetic() || c == '_'),
        _ => false,
    }
}

/// Split SQL into tokens. Comments and whitespace are skipped.
fn tokenize(sql: &str) -> Vec<Tok<'_>> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut toks = Vec::new();
    let mut i = 0;

    while i < len {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if let Some(j) = skip_comment(bytes, i) {
            i = j;
            continue;
        }
        if c == b'\'' {
            let j = skip_quoted(sql, i).unwrap_or(len);
            toks.push(Tok {
                kind: TokKind::Literal,
                text: &sql[i..j],
                start: i,
                end: j,
            });
            i = j;
            continue;
        }
        if c == b'"' || c == b'`' {
            let mut j = i + 1;
            while j < len {
                if bytes[j] == c {
                    if j + 1 < len && bytes[j + 1] == c {
                        j += 2;
                        continue;
                    }
                    break;
                }
                j += 1;
            }
            let inner_end = j.min(len);
            let end = (j + 1).min(len);
            toks.push(Tok {
                kind: TokKind::Ident,
                text: &sql[i + 1..inner_end],
                start: i,
                end,
            });
            i = end;
            continue;
        }
        if c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80 {
            let mut j = i;
            while j < len
                && (bytes[j].is_ascii_alphanumeric()
                    || bytes[j] == b'_'
                    || bytes[j] == b'$'
                    || bytes[j] >= 0x80)
            {
                j += 1;
            }
            toks.push(Tok {
                kind: TokKind::Word,
                text: &sql[i..j],
                start: i,
                end: j,
            });
            i = j;
            continue;
        }
        toks.push(Tok {
            kind: TokKind::Punct,
            text: &sql[i..i + 1],
            start: i,
            end: i + 1,
        });
        i += 1;
    }

    toks
}

/// Split SQL into individual statements, respecting dollar-quoted blocks,
/// string literals, quoted identifiers, and comments.
pub fn split_statements(sql: &str) -> Vec<&str> {
    statement_ranges(sql)
        .into_iter()
        .map(|(s, e)| &sql[s..e])
        .collect()
}

/// Byte ranges of the individual statements in `sql`, each trimmed of
/// surrounding whitespace. Empty statements are skipped.
fn statement_ranges(sql: &str) -> Vec<(usize, usize)> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut i = 0;

    while i < len {
        if let Some(j) = skip_comment(bytes, i) {
            i = j;
            continue;
        }
        if let Some(j) = skip_quoted(sql, i) {
            i = j;
            continue;
        }
        if bytes[i] == b';' {
            if let Some(r) = trim_range(sql, start, i) {
                ranges.push(r);
            }
            i += 1;
            start = i;
            continue;
        }
        i += 1;
    }

    // Remainder after the last semicolon
    if let Some(r) = trim_range(sql, start, len) {
        ranges.push(r);
    }

    ranges
}

/// Narrow `start..end` to the non-whitespace content it contains, or `None`
/// if it is entirely whitespace.
fn trim_range(sql: &str, start: usize, end: usize) -> Option<(usize, usize)> {
    let slice = &sql[start..end];
    if slice.trim().is_empty() {
        return None;
    }
    let lead = slice.len() - slice.trim_start().len();
    let trail = slice.len() - slice.trim_end().len();
    Some((start + lead, end - trail))
}

/// If a comment starts at `i`, return the offset just past it.
///
/// Handles `-- line` comments (terminating before the newline) and nested
/// `/* block */` comments.
fn skip_comment(bytes: &[u8], i: usize) -> Option<usize> {
    let len = bytes.len();
    if bytes[i] == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
        let mut j = i + 2;
        while j < len && bytes[j] != b'\n' {
            j += 1;
        }
        return Some(j);
    }
    if bytes[i] == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
        let mut j = i + 2;
        let mut depth = 1usize;
        while j < len && depth > 0 {
            if j + 1 < len && bytes[j] == b'/' && bytes[j + 1] == b'*' {
                depth += 1;
                j += 2;
            } else if j + 1 < len && bytes[j] == b'*' && bytes[j + 1] == b'/' {
                depth -= 1;
                j += 2;
            } else {
                j += 1;
            }
        }
        return Some(j.min(len));
    }
    None
}

/// If a quoted region starts at `i`, return the offset just past it.
///
/// Covers string literals (including `E'...'` escape strings and doubled-quote
/// escapes), double-quoted / backtick-quoted identifiers, and dollar-quoted
/// blocks.
fn skip_quoted(sql: &str, i: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    match bytes[i] {
        b'\'' => {
            // E'...' escape strings honour backslash escapes.
            let is_escape_string = i > 0
                && (bytes[i - 1] == b'E' || bytes[i - 1] == b'e')
                && (i < 2 || !(bytes[i - 2].is_ascii_alphanumeric() || bytes[i - 2] == b'_'));
            let mut j = i + 1;
            while j < len {
                if is_escape_string && bytes[j] == b'\\' {
                    j += 2;
                    continue;
                }
                if bytes[j] == b'\'' {
                    if j + 1 < len && bytes[j + 1] == b'\'' {
                        j += 2; // doubled-quote escape
                    } else {
                        j += 1;
                        break;
                    }
                } else {
                    j += 1;
                }
            }
            Some(j.min(len))
        }
        q @ (b'"' | b'`') => {
            let mut j = i + 1;
            while j < len {
                if bytes[j] == q {
                    if j + 1 < len && bytes[j + 1] == q {
                        j += 2; // doubled-quote escape
                        continue;
                    }
                    j += 1;
                    break;
                }
                j += 1;
            }
            Some(j.min(len))
        }
        // Dollar-quoted string ($$...$$, $tag$...$tag$)
        b'$' => {
            let tag_start = i;
            let mut j = i + 1;
            while j < len && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            if j < len && bytes[j] == b'$' {
                let tag = &sql[tag_start..=j];
                j += 1;
                while j < len {
                    if bytes[j] == b'$' && sql[j..].starts_with(tag) {
                        j += tag.len();
                        break;
                    }
                    j += 1;
                }
            }
            Some(j.min(len))
        }
        _ => None,
    }
}

/// Blank out every comment in `sql`, preserving byte offsets and line breaks.
///
/// Comment bytes become spaces (newlines are kept) so the result has exactly
/// the same length and line structure as the input. This lets semantic
/// analysis run on comment-free SQL while diagnostics still resolve to the
/// original source position.
pub fn strip_comments(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = bytes.to_vec();
    let mut i = 0;

    while i < len {
        if let Some(j) = skip_comment(bytes, i) {
            for b in &mut out[i..j] {
                if *b != b'\n' {
                    *b = b' ';
                }
            }
            i = j;
            continue;
        }
        if let Some(j) = skip_quoted(sql, i) {
            i = j;
            continue;
        }
        i += 1;
    }

    // Only ASCII comment bytes were replaced with ASCII spaces, so the result
    // is still valid UTF-8.
    String::from_utf8(out).unwrap_or_else(|_| sql.to_string())
}

/// The 1-based line number containing the given byte offset.
pub fn line_number_at(sql: &str, offset: usize) -> usize {
    sql[..offset.min(sql.len())]
        .bytes()
        .filter(|b| *b == b'\n')
        .count()
        + 1
}

/// Split MySQL SQL into individual statements at top-level `;` terminators.
///
/// Respects single-quoted strings, double-quoted strings, backtick-quoted
/// identifiers, single-line `--` comments, and `/* ... */` block comments.
/// Does **not** handle MySQL's `DELIMITER //` blocks — stored-procedure DDL
/// that needs an alternate delimiter must be split by the caller (or
/// re-written without DELIMITER, which works for most ALTER/CREATE patterns).
///
/// Returns owned `String`s rather than borrowed slices so callers can pass
/// them directly to `mysql_async::query_drop` without lifetime gymnastics.
pub fn split_mysql_statements(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < len {
        let c = bytes[i];
        // Line comment
        if c == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Block comment
        if c == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(len);
            continue;
        }
        // Single-quoted string
        if c == b'\'' {
            i += 1;
            while i < len && bytes[i] != b'\'' {
                if bytes[i] == b'\\' && i + 1 < len {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            i += 1;
            continue;
        }
        // Double-quoted string
        if c == b'"' {
            i += 1;
            while i < len && bytes[i] != b'"' {
                if bytes[i] == b'\\' && i + 1 < len {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            i += 1;
            continue;
        }
        // Backtick-quoted identifier
        if c == b'`' {
            i += 1;
            while i < len && bytes[i] != b'`' {
                i += 1;
            }
            i += 1;
            continue;
        }
        // Statement terminator
        if c == b';' {
            out.push(sql[start..i].to_string());
            i += 1;
            start = i;
            continue;
        }
        i += 1;
    }
    let tail = sql[start..].trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_simple_statements() {
        let sql = "SELECT 1; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn test_split_respects_string_literals() {
        let sql = "SELECT 'hello;world'; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts, vec!["SELECT 'hello;world'", "SELECT 2"]);
    }

    #[test]
    fn test_split_respects_dollar_quoting() {
        let sql =
            "CREATE FUNCTION foo() RETURNS void AS $$ BEGIN; END; $$ LANGUAGE plpgsql; SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("BEGIN; END;"));
    }

    #[test]
    fn test_split_respects_tagged_dollar_quoting() {
        let sql = "CREATE FUNCTION foo() RETURNS void AS $body$ BEGIN; END; $body$ LANGUAGE plpgsql; SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("BEGIN; END;"));
    }

    #[test]
    fn test_split_respects_comments() {
        let sql = "-- This is a comment with ; semicolon\nSELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_split_no_trailing_semicolon() {
        let sql = "SELECT 1";
        let stmts = split_statements(sql);
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn test_extract_create_table() {
        let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY);";
        let ops = extract_ddl_operations(sql);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            DdlOperation::CreateTable {
                table,
                if_not_exists,
            } => {
                assert_eq!(table, "users");
                assert!(!if_not_exists);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_extract_create_table_if_not_exists() {
        let sql = "CREATE TABLE IF NOT EXISTS users (id SERIAL);";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateTable {
                table,
                if_not_exists,
            } => {
                assert_eq!(table, "users");
                assert!(if_not_exists);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_extract_add_column() {
        let sql = "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL DEFAULT '';";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::AlterTableAddColumn {
                table,
                column,
                is_not_null,
                has_default,
                ..
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
                assert!(is_not_null);
                assert!(has_default);
            }
            _ => panic!("Expected AlterTableAddColumn"),
        }
    }

    /// Convenience: the single AddColumn op parsed out of `sql`.
    fn add_column(sql: &str) -> DdlOperation {
        let ops = extract_ddl_operations(sql);
        assert_eq!(ops.len(), 1, "expected exactly one op, got {:?}", ops);
        ops.into_iter().next().unwrap()
    }

    #[test]
    fn test_add_column_if_not_exists_names_the_column() {
        match add_column(
            "ALTER TABLE dicom.reid_shares ADD COLUMN IF NOT EXISTS threshold smallint;",
        ) {
            DdlOperation::AlterTableAddColumn {
                table,
                column,
                data_type,
                is_not_null,
                has_default,
                if_not_exists,
                default_expr,
            } => {
                assert_eq!(table, "reid_shares");
                assert_eq!(column, "threshold");
                assert_eq!(data_type, "smallint");
                assert!(default_expr.is_none());
                assert!(!is_not_null);
                assert!(!has_default);
                assert!(if_not_exists);
            }
            other => panic!("Expected AlterTableAddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_add_column_ignores_not_null_in_comments() {
        let sql = "-- Every ceremony writes the threshold NOT NULL.\n\
                   ALTER TABLE dicom.reid_shares\n  \
                     ADD COLUMN IF NOT EXISTS threshold smallint\n    \
                       CHECK (threshold IS NULL OR threshold BETWEEN 1 AND 255);";
        match add_column(sql) {
            DdlOperation::AlterTableAddColumn {
                column,
                is_not_null,
                ..
            } => {
                assert_eq!(column, "threshold");
                assert!(!is_not_null, "NOT NULL came from a comment");
            }
            other => panic!("Expected AlterTableAddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_add_column_ignores_not_null_inside_check() {
        match add_column("ALTER TABLE t ADD COLUMN c text CHECK (c IS NOT NULL);") {
            DdlOperation::AlterTableAddColumn { is_not_null, .. } => assert!(!is_not_null),
            other => panic!("Expected AlterTableAddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_add_column_ignores_keywords_inside_string_literals() {
        match add_column("ALTER TABLE t ADD COLUMN c text DEFAULT 'NOT NULL';") {
            DdlOperation::AlterTableAddColumn {
                is_not_null,
                has_default,
                ..
            } => {
                assert!(!is_not_null);
                assert!(has_default);
            }
            other => panic!("Expected AlterTableAddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_add_column_quoted_and_parenthesised_type() {
        match add_column(
            r#"ALTER TABLE "my schema"."my table" ADD "my col" numeric(10,2) NOT NULL;"#,
        ) {
            DdlOperation::AlterTableAddColumn {
                table,
                column,
                data_type,
                is_not_null,
                ..
            } => {
                assert_eq!(table, "my table");
                assert_eq!(column, "my col");
                assert_eq!(data_type, "numeric(10,2)");
                assert!(is_not_null);
            }
            other => panic!("Expected AlterTableAddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_add_column_multiword_type() {
        match add_column("ALTER TABLE t ADD COLUMN c timestamp with time zone NOT NULL;") {
            DdlOperation::AlterTableAddColumn {
                data_type,
                is_not_null,
                ..
            } => {
                assert_eq!(data_type, "timestamp with time zone");
                assert!(is_not_null);
            }
            other => panic!("Expected AlterTableAddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_add_multiple_columns_in_one_statement() {
        let ops = extract_ddl_operations(
            "ALTER TABLE t ADD COLUMN a int, ADD COLUMN IF NOT EXISTS b text NOT NULL;",
        );
        assert_eq!(ops.len(), 2);
        match (&ops[0], &ops[1]) {
            (
                DdlOperation::AlterTableAddColumn {
                    column: c1,
                    is_not_null: n1,
                    ..
                },
                DdlOperation::AlterTableAddColumn {
                    column: c2,
                    is_not_null: n2,
                    if_not_exists,
                    ..
                },
            ) => {
                assert_eq!(c1, "a");
                assert!(!n1);
                assert_eq!(c2, "b");
                assert!(n2);
                assert!(if_not_exists);
            }
            other => panic!("Expected two AlterTableAddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_add_column_captures_default_expression() {
        for (sql, expected) in [
            (
                "ALTER TABLE t ADD COLUMN c timestamptz DEFAULT now();",
                "now()",
            ),
            ("ALTER TABLE t ADD COLUMN c int NOT NULL DEFAULT 5;", "5"),
            (
                "ALTER TABLE t ADD COLUMN c text DEFAULT 'x' NOT NULL;",
                "'x'",
            ),
            ("ALTER TABLE t ADD COLUMN c text DEFAULT NULL;", "NULL"),
            (
                "ALTER TABLE t ADD COLUMN c text[] DEFAULT '{}' CHECK (c IS NOT NULL);",
                "'{}'",
            ),
        ] {
            match add_column(sql) {
                DdlOperation::AlterTableAddColumn { default_expr, .. } => {
                    assert_eq!(default_expr.as_deref(), Some(expected), "for {}", sql)
                }
                other => panic!("Expected AlterTableAddColumn, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_add_column_default_is_per_clause() {
        let ops = extract_ddl_operations(
            "ALTER TABLE t ADD COLUMN a text[] DEFAULT '{}', ADD COLUMN b timestamptz NOT NULL DEFAULT now();",
        );
        assert_eq!(ops.len(), 2);
        match (&ops[0], &ops[1]) {
            (
                DdlOperation::AlterTableAddColumn {
                    default_expr: a, ..
                },
                DdlOperation::AlterTableAddColumn {
                    default_expr: b, ..
                },
            ) => {
                assert_eq!(a.as_deref(), Some("'{}'"));
                assert_eq!(b.as_deref(), Some("now()"));
            }
            other => panic!("Expected two AlterTableAddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_add_constraint_is_not_parsed_as_a_column() {
        let ops = extract_ddl_operations("ALTER TABLE t ADD CONSTRAINT t_pk PRIMARY KEY (id);");
        assert!(matches!(ops[0], DdlOperation::AddConstraint { .. }));
    }

    #[test]
    fn test_located_ops_point_at_the_column() {
        let sql =
            "-- comment with NOT NULL\n-- another\nALTER TABLE t\n  ADD COLUMN c int NOT NULL;";
        let located = extract_ddl_operations_located(sql);
        assert_eq!(located.len(), 1);
        // Statement starts on line 3, the column token is on line 4.
        assert_eq!(line_number_at(sql, located[0].start), 3);
        assert_eq!(line_number_at(sql, located[0].focus), 4);
        assert_eq!(&sql[located[0].focus..located[0].focus + 1], "c");
    }

    #[test]
    fn test_strip_comments_preserves_offsets_and_lines() {
        let sql = "-- NOT NULL\nSELECT 1; /* NOT NULL */\nSELECT 'not -- a comment';";
        let stripped = strip_comments(sql);
        assert_eq!(stripped.len(), sql.len());
        assert_eq!(stripped.lines().count(), sql.lines().count());
        assert!(!stripped.to_uppercase().contains("NOT NULL"));
        assert!(stripped.contains("'not -- a comment'"));
    }

    #[test]
    fn test_line_number_at_is_one_based() {
        let sql = "a\nb\nc";
        assert_eq!(line_number_at(sql, 0), 1);
        assert_eq!(line_number_at(sql, 2), 2);
        assert_eq!(line_number_at(sql, 4), 3);
    }

    #[test]
    fn test_extract_create_index() {
        let sql = "CREATE UNIQUE INDEX CONCURRENTLY idx_users_email ON users (email);";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateIndex {
                name,
                table,
                is_concurrent,
                is_unique,
            } => {
                assert_eq!(name, "idx_users_email");
                assert_eq!(table, "users");
                assert!(is_concurrent);
                assert!(is_unique);
            }
            _ => panic!("Expected CreateIndex"),
        }
    }

    #[test]
    fn test_extract_create_function() {
        let sql = "CREATE OR REPLACE FUNCTION my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateFunction { name } => {
                assert_eq!(name, "my_func");
            }
            _ => panic!("Expected CreateFunction, got {:?}", ops[0]),
        }
    }

    #[test]
    fn test_extract_create_enum() {
        let sql = "CREATE TYPE mood AS ENUM ('happy', 'sad');";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateEnum { name } => {
                assert_eq!(name, "mood");
            }
            _ => panic!("Expected CreateEnum"),
        }
    }

    #[test]
    fn test_extract_multiple() {
        let sql = "CREATE TABLE users (id SERIAL); CREATE INDEX idx_users ON users (id); DROP TABLE old_table;";
        let ops = extract_ddl_operations(sql);
        assert_eq!(ops.len(), 3);
    }

    #[test]
    fn test_extract_truncate() {
        let sql = "TRUNCATE TABLE users;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::TruncateTable { table } => assert_eq!(table, "users"),
            _ => panic!("Expected TruncateTable"),
        }
    }

    #[test]
    fn test_extract_drop_column() {
        let sql = "ALTER TABLE users DROP COLUMN email;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::AlterTableDropColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
            }
            _ => panic!("Expected AlterTableDropColumn"),
        }
    }

    #[test]
    fn test_extract_alter_column() {
        let sql = "ALTER TABLE users ALTER COLUMN name TYPE text;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::AlterTableAlterColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "name");
            }
            _ => panic!("Expected AlterTableAlterColumn"),
        }
    }

    #[test]
    fn test_extract_materialized_view() {
        let sql = "CREATE MATERIALIZED VIEW user_stats AS SELECT count(*) FROM users;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateView {
                name,
                is_materialized,
            } => {
                assert_eq!(name, "user_stats");
                assert!(is_materialized);
            }
            _ => panic!("Expected CreateView"),
        }
    }

    #[test]
    fn test_block_comment_with_semicolons() {
        let sql = "/* comment; with; semicolons */ SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_escaped_string_quotes() {
        let sql = "SELECT 'it''s; here'; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_respects_e_escape_strings() {
        let sql = r"SELECT E'hello\';world'; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains(r"E'hello\';world'"));
    }

    #[test]
    fn test_split_e_string_with_backslash() {
        let sql = r"SELECT E'it\'s a test; really'; SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_nested_block_comments() {
        let sql = "SELECT /* outer /* inner */ outer */ 1; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_split_whitespace_only() {
        let stmts = split_statements("   \n\t  ");
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_split_comment_only() {
        let stmts = split_statements("-- just a comment\n");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "-- just a comment");
    }

    #[test]
    fn test_split_mixed_e_and_regular_strings() {
        let sql = r"SELECT 'normal;string', E'escape\';string'; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_mysql_basic() {
        let sql = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
        let stmts = split_mysql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CREATE TABLE a"));
        assert!(stmts[1].contains("CREATE TABLE b"));
    }

    #[test]
    fn test_split_mysql_respects_backticks_with_semicolons() {
        // A backtick-quoted identifier with `;` inside should NOT split.
        let sql = "CREATE TABLE `weird;name` (id INT); CREATE TABLE b (id INT);";
        let stmts = split_mysql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("`weird;name`"));
    }

    #[test]
    fn test_split_mysql_respects_string_literals_with_semicolons() {
        let sql = "INSERT INTO t VALUES ('a;b'); INSERT INTO t VALUES ('c;d');";
        let stmts = split_mysql_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_mysql_keeps_leading_comments_with_statement() {
        // The first chunk contains both the comment header and the CREATE TABLE.
        // Splitter doesn't emit comment-only fragments.
        let sql = "-- header comment\nCREATE TABLE a (id INT);\nCREATE TABLE b (id INT);";
        let stmts = split_mysql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CREATE TABLE a"));
    }

    #[test]
    fn test_split_mysql_handles_block_comments() {
        let sql = "/* block ; comment */ CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
        let stmts = split_mysql_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_mysql_no_trailing_semicolon() {
        let sql = "CREATE TABLE a (id INT)";
        let stmts = split_mysql_statements(sql);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("CREATE TABLE a"));
    }
}
