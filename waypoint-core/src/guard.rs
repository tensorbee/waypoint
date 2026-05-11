//! Guard expression parser and evaluator for migration pre/post conditions.
//!
//! Guard expressions are declared in migration file headers using directives:
//! ```sql
//! -- waypoint:require table_exists("users")
//! -- waypoint:require NOT column_exists("users", "email")
//! -- waypoint:ensure column_exists("users", "email")
//! ```
//!
//! Expressions support boolean operators (`AND`, `OR`, `NOT`), comparison
//! operators (`<`, `>`, `<=`, `>=`), and built-in assertion functions that
//! query the database schema.

use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::{Result, WaypointError};

/// Maximum nesting depth for guard expression parsing.
const MAX_PARSE_DEPTH: usize = 50;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Behavior when a `-- waypoint:require` precondition fails.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum OnRequireFail {
    /// Abort the migration with an error (default).
    #[default]
    Error,
    /// Log a warning but continue with the migration.
    Warn,
    /// Silently skip the migration.
    Skip,
}

impl std::str::FromStr for OnRequireFail {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "error" => Ok(Self::Error),
            "warn" => Ok(Self::Warn),
            "skip" => Ok(Self::Skip),
            other => Err(format!("unknown on_require_fail value: '{other}'")),
        }
    }
}

/// Configuration for guard (pre/post condition) evaluation.
#[derive(Debug, Clone)]
pub struct GuardsConfig {
    /// Whether guard conditions are evaluated before/after migrations.
    pub enabled: bool,
    /// What to do when a precondition (`-- waypoint:require`) fails.
    pub on_require_fail: OnRequireFail,
}

impl Default for GuardsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            on_require_fail: OnRequireFail::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

/// A comparison operator in a guard expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonOp {
    /// `<`
    Lt,
    /// `>`
    Gt,
    /// `<=`
    Le,
    /// `>=`
    Ge,
}

impl std::fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComparisonOp::Lt => write!(f, "<"),
            ComparisonOp::Gt => write!(f, ">"),
            ComparisonOp::Le => write!(f, "<="),
            ComparisonOp::Ge => write!(f, ">="),
        }
    }
}

/// A node in the guard expression abstract syntax tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardExpr {
    /// A call to a built-in assertion function, e.g. `table_exists("users")`.
    FunctionCall {
        /// Function name (e.g. `table_exists`, `column_exists`).
        name: String,
        /// Argument expressions.
        args: Vec<GuardExpr>,
    },
    /// Logical AND of two expressions.
    And(Box<GuardExpr>, Box<GuardExpr>),
    /// Logical OR of two expressions.
    Or(Box<GuardExpr>, Box<GuardExpr>),
    /// Logical NOT of an expression.
    Not(Box<GuardExpr>),
    /// A comparison between two expressions.
    Comparison {
        /// Left-hand operand.
        left: Box<GuardExpr>,
        /// Comparison operator.
        op: ComparisonOp,
        /// Right-hand operand.
        right: Box<GuardExpr>,
    },
    /// A string literal (double-quoted).
    StringLiteral(String),
    /// A numeric literal.
    NumberLiteral(i64),
    /// A boolean literal (`true` / `false`).
    BoolLiteral(bool),
}

/// The runtime value produced by evaluating a guard expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardValue {
    /// A boolean value.
    Bool(bool),
    /// A numeric (integer) value.
    Number(i64),
    /// A string value.
    Str(String),
}

impl std::fmt::Display for GuardValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GuardValue::Bool(b) => write!(f, "{b}"),
            GuardValue::Number(n) => write!(f, "{n}"),
            GuardValue::Str(s) => write!(f, "\"{s}\""),
        }
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

/// A token produced by the lexer.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Ident(String),
    StringLit(String),
    NumberLit(i64),
    And,
    Or,
    Not,
    Lt,
    Gt,
    Le,
    Ge,
    LParen,
    RParen,
    Comma,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Ident(s) => write!(f, "{s}"),
            Token::StringLit(s) => write!(f, "\"{s}\""),
            Token::NumberLit(n) => write!(f, "{n}"),
            Token::And => write!(f, "AND"),
            Token::Or => write!(f, "OR"),
            Token::Not => write!(f, "NOT"),
            Token::Lt => write!(f, "<"),
            Token::Gt => write!(f, ">"),
            Token::Le => write!(f, "<="),
            Token::Ge => write!(f, ">="),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::Comma => write!(f, ","),
        }
    }
}

/// Tokenize a guard expression string into a sequence of tokens.
fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let ch = chars[i];

        // Skip whitespace
        if ch.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // String literal (double-quoted)
        if ch == '"' {
            i += 1;
            let start = i;
            while i < len && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < len {
                    i += 2; // skip escaped character
                } else {
                    i += 1;
                }
            }
            if i >= len {
                return Err(WaypointError::ConfigError(
                    "Guard expression: unterminated string literal".to_string(),
                ));
            }
            let s: String = chars[start..i].iter().collect();
            tokens.push(Token::StringLit(s));
            i += 1; // skip closing quote
            continue;
        }

        // Parentheses and comma
        if ch == '(' {
            tokens.push(Token::LParen);
            i += 1;
            continue;
        }
        if ch == ')' {
            tokens.push(Token::RParen);
            i += 1;
            continue;
        }
        if ch == ',' {
            tokens.push(Token::Comma);
            i += 1;
            continue;
        }

        // Comparison operators
        if ch == '<' {
            if i + 1 < len && chars[i + 1] == '=' {
                tokens.push(Token::Le);
                i += 2;
            } else {
                tokens.push(Token::Lt);
                i += 1;
            }
            continue;
        }
        if ch == '>' {
            if i + 1 < len && chars[i + 1] == '=' {
                tokens.push(Token::Ge);
                i += 2;
            } else {
                tokens.push(Token::Gt);
                i += 1;
            }
            continue;
        }

        // Numbers
        if ch.is_ascii_digit() {
            let start = i;
            while i < len && chars[i].is_ascii_digit() {
                i += 1;
            }
            let num_str: String = chars[start..i].iter().collect();
            let n = num_str.parse::<i64>().map_err(|e| {
                WaypointError::ConfigError(format!(
                    "Guard expression: invalid number '{num_str}': {e}"
                ))
            })?;
            tokens.push(Token::NumberLit(n));
            continue;
        }

        // Identifiers and keywords (AND, OR, NOT, true, false)
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = i;
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            if word.eq_ignore_ascii_case("AND") {
                tokens.push(Token::And);
            } else if word.eq_ignore_ascii_case("OR") {
                tokens.push(Token::Or);
            } else if word.eq_ignore_ascii_case("NOT") {
                tokens.push(Token::Not);
            } else if word.eq_ignore_ascii_case("TRUE") {
                tokens.push(Token::Ident("true".to_string()));
            } else if word.eq_ignore_ascii_case("FALSE") {
                tokens.push(Token::Ident("false".to_string()));
            } else {
                tokens.push(Token::Ident(word));
            }
            continue;
        }

        return Err(WaypointError::ConfigError(format!(
            "Guard expression: unexpected character '{ch}'"
        )));
    }

    Ok(tokens)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Recursive descent parser state.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        match self.advance() {
            Some(ref tok) if tok == expected => Ok(()),
            Some(tok) => Err(WaypointError::ConfigError(format!(
                "Guard expression: expected '{expected}', found '{tok}'"
            ))),
            None => Err(WaypointError::ConfigError(format!(
                "Guard expression: expected '{expected}', found end of input"
            ))),
        }
    }

    /// Parse a complete expression.
    ///
    /// Grammar: `expr → or_expr`
    fn parse_expr(&mut self, depth: usize) -> Result<GuardExpr> {
        self.parse_or_expr(depth)
    }

    /// `or_expr → and_expr (OR and_expr)*`
    fn parse_or_expr(&mut self, depth: usize) -> Result<GuardExpr> {
        if depth > MAX_PARSE_DEPTH {
            return Err(WaypointError::ConfigError(
                "Guard expression: maximum nesting depth exceeded".to_string(),
            ));
        }
        let mut left = self.parse_and_expr(depth + 1)?;
        while self.peek() == Some(&Token::Or) {
            self.advance(); // consume OR
            let right = self.parse_and_expr(depth + 1)?;
            left = GuardExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// `and_expr → not_expr (AND not_expr)*`
    fn parse_and_expr(&mut self, depth: usize) -> Result<GuardExpr> {
        if depth > MAX_PARSE_DEPTH {
            return Err(WaypointError::ConfigError(
                "Guard expression: maximum nesting depth exceeded".to_string(),
            ));
        }
        let mut left = self.parse_not_expr(depth + 1)?;
        while self.peek() == Some(&Token::And) {
            self.advance(); // consume AND
            let right = self.parse_not_expr(depth + 1)?;
            left = GuardExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// `not_expr → NOT not_expr | comparison`
    fn parse_not_expr(&mut self, depth: usize) -> Result<GuardExpr> {
        if depth > MAX_PARSE_DEPTH {
            return Err(WaypointError::ConfigError(
                "Guard expression: maximum nesting depth exceeded".to_string(),
            ));
        }
        if self.peek() == Some(&Token::Not) {
            self.advance(); // consume NOT
            let inner = self.parse_not_expr(depth + 1)?;
            Ok(GuardExpr::Not(Box::new(inner)))
        } else {
            self.parse_comparison(depth + 1)
        }
    }

    /// `comparison → primary ((< | > | <= | >=) primary)?`
    fn parse_comparison(&mut self, depth: usize) -> Result<GuardExpr> {
        if depth > MAX_PARSE_DEPTH {
            return Err(WaypointError::ConfigError(
                "Guard expression: maximum nesting depth exceeded".to_string(),
            ));
        }
        let left = self.parse_primary(depth + 1)?;

        let op = match self.peek() {
            Some(Token::Lt) => Some(ComparisonOp::Lt),
            Some(Token::Gt) => Some(ComparisonOp::Gt),
            Some(Token::Le) => Some(ComparisonOp::Le),
            Some(Token::Ge) => Some(ComparisonOp::Ge),
            _ => None,
        };

        if let Some(op) = op {
            self.advance(); // consume operator
            let right = self.parse_primary(depth + 1)?;
            Ok(GuardExpr::Comparison {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    /// `primary → function_call | '(' expr ')' | literal`
    fn parse_primary(&mut self, depth: usize) -> Result<GuardExpr> {
        if depth > MAX_PARSE_DEPTH {
            return Err(WaypointError::ConfigError(
                "Guard expression: maximum nesting depth exceeded".to_string(),
            ));
        }
        match self.peek().cloned() {
            Some(Token::Ident(name)) => {
                // Check if it's a boolean literal
                if name == "true" {
                    self.advance();
                    return Ok(GuardExpr::BoolLiteral(true));
                }
                if name == "false" {
                    self.advance();
                    return Ok(GuardExpr::BoolLiteral(false));
                }

                // Check if it's a function call (ident followed by '(')
                if self.pos + 1 < self.tokens.len() && self.tokens[self.pos + 1] == Token::LParen {
                    self.advance(); // consume ident
                    self.advance(); // consume '('
                    let args = self.parse_args(depth + 1)?;
                    self.expect(&Token::RParen)?;
                    Ok(GuardExpr::FunctionCall { name, args })
                } else {
                    Err(WaypointError::ConfigError(format!(
                        "Guard expression: unexpected identifier '{name}' (expected function call)"
                    )))
                }
            }
            Some(Token::LParen) => {
                self.advance(); // consume '('
                let expr = self.parse_expr(depth + 1)?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Some(Token::StringLit(s)) => {
                self.advance();
                Ok(GuardExpr::StringLiteral(s))
            }
            Some(Token::NumberLit(n)) => {
                self.advance();
                Ok(GuardExpr::NumberLiteral(n))
            }
            Some(tok) => Err(WaypointError::ConfigError(format!(
                "Guard expression: unexpected token '{tok}'"
            ))),
            None => Err(WaypointError::ConfigError(
                "Guard expression: unexpected end of input".to_string(),
            )),
        }
    }

    /// `args → expr (',' expr)* | ε`
    fn parse_args(&mut self, depth: usize) -> Result<Vec<GuardExpr>> {
        let mut args = Vec::new();

        // Empty argument list
        if self.peek() == Some(&Token::RParen) {
            return Ok(args);
        }

        args.push(self.parse_expr(depth)?);

        while self.peek() == Some(&Token::Comma) {
            self.advance(); // consume ','
            args.push(self.parse_expr(depth)?);
        }

        Ok(args)
    }
}

/// Parse a guard expression string into an AST.
///
/// # Errors
///
/// Returns `WaypointError::ConfigError` if the expression has invalid syntax.
///
/// # Examples
///
/// ```
/// use waypoint_core::guard::parse;
///
/// let expr = parse("table_exists(\"users\")").unwrap();
/// let expr = parse("table_exists(\"users\") AND column_exists(\"users\", \"email\")").unwrap();
/// let expr = parse("NOT table_exists(\"legacy\")").unwrap();
/// let expr = parse("row_count(\"users\") < 1000").unwrap();
/// ```
pub fn parse(input: &str) -> Result<GuardExpr> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(WaypointError::ConfigError(
            "Guard expression: empty expression".to_string(),
        ));
    }
    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expr(0)?;

    // Ensure all tokens were consumed
    if parser.pos < parser.tokens.len() {
        let remaining = &parser.tokens[parser.pos];
        return Err(WaypointError::ConfigError(format!(
            "Guard expression: unexpected token '{remaining}' after complete expression"
        )));
    }

    Ok(expr)
}

// ---------------------------------------------------------------------------
// Built-in function SQL generation
// ---------------------------------------------------------------------------

/// Generate the SQL query for a built-in guard function (PostgreSQL).
///
/// Returns `(sql, params, is_boolean)` — `params` contains the parameter values
/// in order ($1, $2, $3...), and `is_boolean` is `true` when the query returns
/// a single boolean, `false` when it returns a count (Number).
#[cfg(feature = "postgres")]
fn builtin_sql(name: &str, args: &[String], schema: &str) -> Result<(String, Vec<String>, bool)> {
    match name {
        "table_exists" => {
            require_args(name, args, 1)?;
            let table = &args[0];
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = $2)"
                    .to_string(),
                vec![schema.to_string(), table.to_string()],
                true,
            ))
        }
        "column_exists" => {
            require_args(name, args, 2)?;
            let table = &args[0];
            let column = &args[1];
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                 AND column_name = $3)"
                    .to_string(),
                vec![schema.to_string(), table.to_string(), column.to_string()],
                true,
            ))
        }
        "column_type" => {
            require_args(name, args, 3)?;
            let table = &args[0];
            let column = &args[1];
            let expected_type = &args[2];
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                 AND column_name = $3 AND data_type = $4)"
                    .to_string(),
                vec![
                    schema.to_string(),
                    table.to_string(),
                    column.to_string(),
                    expected_type.to_string(),
                ],
                true,
            ))
        }
        "column_nullable" => {
            require_args(name, args, 2)?;
            let table = &args[0];
            let column = &args[1];
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                 AND column_name = $3 AND is_nullable = 'YES')"
                    .to_string(),
                vec![schema.to_string(), table.to_string(), column.to_string()],
                true,
            ))
        }
        "index_exists" => {
            require_args(name, args, 1)?;
            let index = &args[0];
            Ok((
                "SELECT EXISTS(SELECT 1 FROM pg_indexes \
                 WHERE schemaname = $1 AND indexname = $2)"
                    .to_string(),
                vec![schema.to_string(), index.to_string()],
                true,
            ))
        }
        "constraint_exists" => {
            require_args(name, args, 2)?;
            let table = &args[0];
            let constraint = &args[1];
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.table_constraints \
                 WHERE table_schema = $1 AND table_name = $2 \
                 AND constraint_name = $3)"
                    .to_string(),
                vec![
                    schema.to_string(),
                    table.to_string(),
                    constraint.to_string(),
                ],
                true,
            ))
        }
        "function_exists" => {
            require_args(name, args, 1)?;
            let func = &args[0];
            Ok((
                "SELECT EXISTS(SELECT 1 FROM pg_proc p \
                 JOIN pg_namespace n ON n.oid = p.pronamespace \
                 WHERE n.nspname = $1 AND p.proname = $2)"
                    .to_string(),
                vec![schema.to_string(), func.to_string()],
                true,
            ))
        }
        "enum_exists" => {
            require_args(name, args, 1)?;
            let enum_name = &args[0];
            Ok((
                "SELECT EXISTS(SELECT 1 FROM pg_type t \
                 JOIN pg_namespace n ON n.oid = t.typnamespace \
                 WHERE n.nspname = $1 AND t.typname = $2 \
                 AND t.typtype = 'e')"
                    .to_string(),
                vec![schema.to_string(), enum_name.to_string()],
                true,
            ))
        }
        "row_count" => {
            require_args(name, args, 1)?;
            let table = &args[0];
            Ok((
                "SELECT COALESCE(n_live_tup, 0)::bigint FROM pg_stat_user_tables \
                 WHERE schemaname = $1 AND relname = $2"
                    .to_string(),
                vec![schema.to_string(), table.to_string()],
                false,
            ))
        }
        "sql" => {
            require_args(name, args, 1)?;
            let query = &args[0];
            Ok((query.to_string(), vec![], true))
        }
        _ => Err(WaypointError::ConfigError(format!(
            "Guard expression: unknown function '{name}'"
        ))),
    }
}

/// Generate the SQL query for a built-in guard function (MySQL 8.0+).
///
/// Mirrors [`builtin_sql`] but emits `?` placeholders and uses MySQL system
/// tables (`information_schema.*`). The `enum_exists` builtin is rejected
/// because MySQL has no enum *type* — ENUM is a column type modifier and
/// can't exist independently in the schema.
#[cfg(feature = "mysql")]
fn builtin_sql_mysql(
    name: &str,
    args: &[String],
    schema: &str,
) -> Result<(String, Vec<String>, bool)> {
    match name {
        "table_exists" => {
            require_args(name, args, 1)?;
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = ? AND table_name = ?)"
                    .to_string(),
                vec![schema.to_string(), args[0].clone()],
                true,
            ))
        }
        "column_exists" => {
            require_args(name, args, 2)?;
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = ? AND table_name = ? AND column_name = ?)"
                    .to_string(),
                vec![schema.to_string(), args[0].clone(), args[1].clone()],
                true,
            ))
        }
        "column_type" => {
            require_args(name, args, 3)?;
            // MySQL stores the base type in DATA_TYPE (e.g. "varchar", "int")
            // and the full declaration in COLUMN_TYPE (e.g. "varchar(255)").
            // We match DATA_TYPE for consistency with the PG behaviour where
            // `column_type("t","c","character varying")` matches the type name.
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = ? AND table_name = ? \
                 AND column_name = ? AND data_type = ?)"
                    .to_string(),
                vec![
                    schema.to_string(),
                    args[0].clone(),
                    args[1].clone(),
                    args[2].clone(),
                ],
                true,
            ))
        }
        "column_nullable" => {
            require_args(name, args, 2)?;
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = ? AND table_name = ? \
                 AND column_name = ? AND is_nullable = 'YES')"
                    .to_string(),
                vec![schema.to_string(), args[0].clone(), args[1].clone()],
                true,
            ))
        }
        "index_exists" => {
            require_args(name, args, 1)?;
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.statistics \
                 WHERE table_schema = ? AND index_name = ?)"
                    .to_string(),
                vec![schema.to_string(), args[0].clone()],
                true,
            ))
        }
        "constraint_exists" => {
            require_args(name, args, 2)?;
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.table_constraints \
                 WHERE table_schema = ? AND table_name = ? AND constraint_name = ?)"
                    .to_string(),
                vec![schema.to_string(), args[0].clone(), args[1].clone()],
                true,
            ))
        }
        "function_exists" => {
            require_args(name, args, 1)?;
            Ok((
                "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
                 WHERE routine_schema = ? AND routine_name = ? \
                 AND routine_type = 'FUNCTION')"
                    .to_string(),
                vec![schema.to_string(), args[0].clone()],
                true,
            ))
        }
        "enum_exists" => Err(WaypointError::ConfigError(
            "Guard expression: enum_exists() is not supported on MySQL — \
             MySQL has no enum *type* (ENUM is a column type modifier, not a \
             schema object). Use column_type(..., \"enum\") instead."
                .into(),
        )),
        "row_count" => {
            require_args(name, args, 1)?;
            // information_schema.tables.table_rows is an approximate count
            // (storage-engine dependent). InnoDB returns NULL for empty/new
            // tables in some cases — COALESCE so callers get 0 rather than
            // a NULL surfacing as a type-conversion error.
            Ok((
                "SELECT COALESCE(table_rows, 0) FROM information_schema.tables \
                 WHERE table_schema = ? AND table_name = ?"
                    .to_string(),
                vec![schema.to_string(), args[0].clone()],
                false,
            ))
        }
        "sql" => {
            require_args(name, args, 1)?;
            Ok((args[0].clone(), vec![], true))
        }
        _ => Err(WaypointError::ConfigError(format!(
            "Guard expression: unknown function '{name}'"
        ))),
    }
}

/// Validate that a function received the expected number of string arguments.
fn require_args(name: &str, args: &[String], expected: usize) -> Result<()> {
    if args.len() != expected {
        return Err(WaypointError::ConfigError(format!(
            "Guard expression: {name}() expects {expected} argument(s), got {}",
            args.len()
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

/// Extract string values from evaluated argument expressions.
///
/// This resolves each argument expression; only `StringLiteral` nodes are
/// accepted as function arguments for built-in functions.
fn extract_string_args(args: &[GuardExpr]) -> Result<Vec<String>> {
    let mut result = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            GuardExpr::StringLiteral(s) => result.push(s.clone()),
            other => {
                return Err(WaypointError::ConfigError(format!(
                    "Guard expression: expected string argument, found {other:?}"
                )));
            }
        }
    }
    Ok(result)
}

/// Evaluate a guard expression tree against a live database.
///
/// Built-in functions are translated to SQL queries and executed against the
/// given `schema`. Boolean operators are short-circuit evaluated.
///
/// # Errors
///
/// Returns `WaypointError::GuardFailed` when a function execution fails, or
/// `WaypointError::ConfigError` for type mismatches and unknown functions.
#[cfg(feature = "postgres")]
pub async fn evaluate(
    client: &tokio_postgres::Client,
    schema: &str,
    expr: &GuardExpr,
) -> Result<bool> {
    let value = eval_expr(client, schema, expr).await?;
    match value {
        GuardValue::Bool(b) => Ok(b),
        other => Err(WaypointError::ConfigError(format!(
            "Guard expression: expected boolean result, got {other}"
        ))),
    }
}

/// Recursively evaluate an expression node, returning its value (PostgreSQL).
#[cfg(feature = "postgres")]
fn eval_expr<'a>(
    client: &'a tokio_postgres::Client,
    schema: &'a str,
    expr: &'a GuardExpr,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GuardValue>> + Send + 'a>> {
    Box::pin(async move {
        match expr {
            GuardExpr::BoolLiteral(b) => Ok(GuardValue::Bool(*b)),
            GuardExpr::NumberLiteral(n) => Ok(GuardValue::Number(*n)),
            GuardExpr::StringLiteral(s) => Ok(GuardValue::Str(s.clone())),

            GuardExpr::Not(inner) => {
                let val = eval_expr(client, schema, inner).await?;
                match val {
                    GuardValue::Bool(b) => Ok(GuardValue::Bool(!b)),
                    other => Err(WaypointError::ConfigError(format!(
                        "Guard expression: NOT requires boolean, got {other}"
                    ))),
                }
            }

            GuardExpr::And(left, right) => {
                let lval = eval_expr(client, schema, left).await?;
                match lval {
                    GuardValue::Bool(false) => Ok(GuardValue::Bool(false)),
                    GuardValue::Bool(true) => {
                        let rval = eval_expr(client, schema, right).await?;
                        match rval {
                            GuardValue::Bool(b) => Ok(GuardValue::Bool(b)),
                            other => Err(WaypointError::ConfigError(format!(
                                "Guard expression: AND requires boolean operands, got {other}"
                            ))),
                        }
                    }
                    other => Err(WaypointError::ConfigError(format!(
                        "Guard expression: AND requires boolean operands, got {other}"
                    ))),
                }
            }

            GuardExpr::Or(left, right) => {
                let lval = eval_expr(client, schema, left).await?;
                match lval {
                    GuardValue::Bool(true) => Ok(GuardValue::Bool(true)),
                    GuardValue::Bool(false) => {
                        let rval = eval_expr(client, schema, right).await?;
                        match rval {
                            GuardValue::Bool(b) => Ok(GuardValue::Bool(b)),
                            other => Err(WaypointError::ConfigError(format!(
                                "Guard expression: OR requires boolean operands, got {other}"
                            ))),
                        }
                    }
                    other => Err(WaypointError::ConfigError(format!(
                        "Guard expression: OR requires boolean operands, got {other}"
                    ))),
                }
            }

            GuardExpr::Comparison { left, op, right } => {
                let lval = eval_expr(client, schema, left).await?;
                let rval = eval_expr(client, schema, right).await?;
                match (&lval, &rval) {
                    (GuardValue::Number(a), GuardValue::Number(b)) => {
                        let result = match op {
                            ComparisonOp::Lt => a < b,
                            ComparisonOp::Gt => a > b,
                            ComparisonOp::Le => a <= b,
                            ComparisonOp::Ge => a >= b,
                        };
                        Ok(GuardValue::Bool(result))
                    }
                    _ => Err(WaypointError::ConfigError(format!(
                        "Guard expression: comparison requires numeric operands, got {lval} {op} {rval}"
                    ))),
                }
            }

            GuardExpr::FunctionCall { name, args } => {
                let string_args = extract_string_args(args)?;
                let (sql, param_values, is_boolean) = builtin_sql(name, &string_args, schema)?;
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = param_values
                    .iter()
                    .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();

                let row = client.query_one(&sql, &params).await.map_err(|e| {
                    WaypointError::GuardFailed {
                        kind: "evaluation".to_string(),
                        script: String::new(),
                        expression: format!(
                            "{name}({}) failed: {e}",
                            string_args
                                .iter()
                                .map(|a| format!("\"{a}\""))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    }
                })?;

                if is_boolean {
                    let val: bool = row.get(0);
                    Ok(GuardValue::Bool(val))
                } else {
                    let val: i64 = row.get(0);
                    Ok(GuardValue::Number(val))
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Dialect-aware evaluator
// ---------------------------------------------------------------------------

/// Evaluate a guard expression against a [`DbClient`] (dialect-aware entry).
///
/// Dispatches to the PostgreSQL or MySQL implementation based on the connection
/// kind. Recursion shape mirrors the legacy [`evaluate`] function; only the
/// leaf `FunctionCall` arm differs per engine.
pub async fn evaluate_db(client: &DbClient, schema: &str, expr: &GuardExpr) -> Result<bool> {
    let value = eval_expr_db(client, schema, expr).await?;
    match value {
        GuardValue::Bool(b) => Ok(b),
        other => Err(WaypointError::ConfigError(format!(
            "Guard expression: expected boolean result, got {other}"
        ))),
    }
}

fn eval_expr_db<'a>(
    client: &'a DbClient,
    schema: &'a str,
    expr: &'a GuardExpr,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GuardValue>> + Send + 'a>> {
    Box::pin(async move {
        match expr {
            GuardExpr::BoolLiteral(b) => Ok(GuardValue::Bool(*b)),
            GuardExpr::NumberLiteral(n) => Ok(GuardValue::Number(*n)),
            GuardExpr::StringLiteral(s) => Ok(GuardValue::Str(s.clone())),

            GuardExpr::Not(inner) => {
                let val = eval_expr_db(client, schema, inner).await?;
                match val {
                    GuardValue::Bool(b) => Ok(GuardValue::Bool(!b)),
                    other => Err(WaypointError::ConfigError(format!(
                        "Guard expression: NOT requires boolean, got {other}"
                    ))),
                }
            }

            GuardExpr::And(left, right) => {
                let lval = eval_expr_db(client, schema, left).await?;
                match lval {
                    GuardValue::Bool(false) => Ok(GuardValue::Bool(false)),
                    GuardValue::Bool(true) => {
                        let rval = eval_expr_db(client, schema, right).await?;
                        match rval {
                            GuardValue::Bool(b) => Ok(GuardValue::Bool(b)),
                            other => Err(WaypointError::ConfigError(format!(
                                "Guard expression: AND requires boolean operands, got {other}"
                            ))),
                        }
                    }
                    other => Err(WaypointError::ConfigError(format!(
                        "Guard expression: AND requires boolean operands, got {other}"
                    ))),
                }
            }

            GuardExpr::Or(left, right) => {
                let lval = eval_expr_db(client, schema, left).await?;
                match lval {
                    GuardValue::Bool(true) => Ok(GuardValue::Bool(true)),
                    GuardValue::Bool(false) => {
                        let rval = eval_expr_db(client, schema, right).await?;
                        match rval {
                            GuardValue::Bool(b) => Ok(GuardValue::Bool(b)),
                            other => Err(WaypointError::ConfigError(format!(
                                "Guard expression: OR requires boolean operands, got {other}"
                            ))),
                        }
                    }
                    other => Err(WaypointError::ConfigError(format!(
                        "Guard expression: OR requires boolean operands, got {other}"
                    ))),
                }
            }

            GuardExpr::Comparison { left, op, right } => {
                let lval = eval_expr_db(client, schema, left).await?;
                let rval = eval_expr_db(client, schema, right).await?;
                match (&lval, &rval) {
                    (GuardValue::Number(a), GuardValue::Number(b)) => {
                        let result = match op {
                            ComparisonOp::Lt => a < b,
                            ComparisonOp::Gt => a > b,
                            ComparisonOp::Le => a <= b,
                            ComparisonOp::Ge => a >= b,
                        };
                        Ok(GuardValue::Bool(result))
                    }
                    _ => Err(WaypointError::ConfigError(format!(
                        "Guard expression: comparison requires numeric operands, got {lval} {op} {rval}"
                    ))),
                }
            }

            GuardExpr::FunctionCall { name, args } => {
                let string_args = extract_string_args(args)?;
                exec_builtin(client, schema, name, &string_args).await
            }
        }
    })
}

/// Execute a built-in guard function against the configured backend.
async fn exec_builtin(
    client: &DbClient,
    schema: &str,
    name: &str,
    string_args: &[String],
) -> Result<GuardValue> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            let (sql, param_values, is_boolean) = builtin_sql(name, string_args, schema)?;
            let pg = client.as_postgres()?;
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = param_values
                .iter()
                .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            let row = pg
                .query_one(&sql, &params)
                .await
                .map_err(|e| guard_failed(name, string_args, &e.to_string()))?;
            if is_boolean {
                Ok(GuardValue::Bool(row.get(0)))
            } else {
                Ok(GuardValue::Number(row.get(0)))
            }
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => {
            use mysql_async::prelude::*;
            let (sql, param_values, is_boolean) = builtin_sql_mysql(name, string_args, schema)?;
            let pool = client.as_mysql()?;
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| guard_failed(name, string_args, &e.to_string()))?;

            // information_schema EXISTS(...) and COUNT(*) both return a single
            // i64 column on MySQL — read as Option<i64> to share the param-
            // binding path between the boolean and numeric builtins (avoids the
            // chrono-feature ambiguity around bool decoding).
            let result: Option<i64> = if param_values.is_empty() {
                conn.query_first(&sql).await
            } else {
                let params: Vec<mysql_async::Value> = param_values
                    .iter()
                    .map(|s| mysql_async::Value::Bytes(s.as_bytes().to_vec()))
                    .collect();
                conn.exec_first(&sql, params).await
            }
            .map_err(|e| guard_failed(name, string_args, &e.to_string()))?;

            if is_boolean {
                Ok(GuardValue::Bool(matches!(result, Some(n) if n != 0)))
            } else {
                // COUNT(*) always returns a row, so None here means a malformed
                // builtin query — treat as 0 rather than panic.
                Ok(GuardValue::Number(result.unwrap_or(0)))
            }
        }
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in".into(),
        )),
    }
}

fn guard_failed(name: &str, args: &[String], reason: &str) -> WaypointError {
    WaypointError::GuardFailed {
        kind: "evaluation".to_string(),
        script: String::new(),
        expression: format!(
            "{name}({}) failed: {reason}",
            args.iter()
                .map(|a| format!("\"{a}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Many of these reference the PG-specific `builtin_sql`. They cover the
// engine-agnostic parser too, but gating individual tests would be noisier
// than gating the module; the parser is covered under both the default
// (postgres) and `--features mysql` (postgres+mysql) builds.
#[cfg(all(test, feature = "postgres"))]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_function_call() {
        let expr = parse("table_exists(\"users\")").unwrap();
        match expr {
            GuardExpr::FunctionCall { name, args } => {
                assert_eq!(name, "table_exists");
                assert_eq!(args.len(), 1);
                assert_eq!(args[0], GuardExpr::StringLiteral("users".to_string()));
            }
            other => panic!("Expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_function_with_multiple_args() {
        let expr = parse("column_exists(\"users\", \"email\")").unwrap();
        match expr {
            GuardExpr::FunctionCall { name, args } => {
                assert_eq!(name, "column_exists");
                assert_eq!(args.len(), 2);
                assert_eq!(args[0], GuardExpr::StringLiteral("users".to_string()));
                assert_eq!(args[1], GuardExpr::StringLiteral("email".to_string()));
            }
            other => panic!("Expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_function_with_three_args() {
        let expr = parse("column_type(\"users\", \"age\", \"integer\")").unwrap();
        match expr {
            GuardExpr::FunctionCall { name, args } => {
                assert_eq!(name, "column_type");
                assert_eq!(args.len(), 3);
                assert_eq!(args[0], GuardExpr::StringLiteral("users".to_string()));
                assert_eq!(args[1], GuardExpr::StringLiteral("age".to_string()));
                assert_eq!(args[2], GuardExpr::StringLiteral("integer".to_string()));
            }
            other => panic!("Expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_and_expression() {
        let expr =
            parse("table_exists(\"users\") AND column_exists(\"users\", \"email\")").unwrap();
        match expr {
            GuardExpr::And(left, right) => {
                match *left {
                    GuardExpr::FunctionCall { ref name, .. } => assert_eq!(name, "table_exists"),
                    ref other => panic!("Expected FunctionCall on left, got {other:?}"),
                }
                match *right {
                    GuardExpr::FunctionCall { ref name, .. } => {
                        assert_eq!(name, "column_exists")
                    }
                    ref other => panic!("Expected FunctionCall on right, got {other:?}"),
                }
            }
            other => panic!("Expected And, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_or_expression() {
        let expr = parse("table_exists(\"users\") OR table_exists(\"accounts\")").unwrap();
        match expr {
            GuardExpr::Or(left, right) => {
                match *left {
                    GuardExpr::FunctionCall { ref name, ref args } => {
                        assert_eq!(name, "table_exists");
                        assert_eq!(args[0], GuardExpr::StringLiteral("users".to_string()));
                    }
                    ref other => panic!("Expected FunctionCall on left, got {other:?}"),
                }
                match *right {
                    GuardExpr::FunctionCall { ref name, ref args } => {
                        assert_eq!(name, "table_exists");
                        assert_eq!(args[0], GuardExpr::StringLiteral("accounts".to_string()));
                    }
                    ref other => panic!("Expected FunctionCall on right, got {other:?}"),
                }
            }
            other => panic!("Expected Or, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_not_expression() {
        let expr = parse("NOT table_exists(\"legacy\")").unwrap();
        match expr {
            GuardExpr::Not(inner) => match *inner {
                GuardExpr::FunctionCall { ref name, .. } => assert_eq!(name, "table_exists"),
                ref other => panic!("Expected FunctionCall inside NOT, got {other:?}"),
            },
            other => panic!("Expected Not, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_double_not() {
        let expr = parse("NOT NOT table_exists(\"t\")").unwrap();
        match expr {
            GuardExpr::Not(inner) => match *inner {
                GuardExpr::Not(inner2) => match *inner2 {
                    GuardExpr::FunctionCall { ref name, .. } => {
                        assert_eq!(name, "table_exists")
                    }
                    ref other => panic!("Expected FunctionCall, got {other:?}"),
                },
                ref other => panic!("Expected Not, got {other:?}"),
            },
            other => panic!("Expected Not, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_nested_parentheses() {
        let expr =
            parse("(table_exists(\"a\") AND table_exists(\"b\")) OR table_exists(\"c\")").unwrap();
        match expr {
            GuardExpr::Or(left, right) => {
                match *left {
                    GuardExpr::And(_, _) => {} // good
                    ref other => panic!("Expected And on left, got {other:?}"),
                }
                match *right {
                    GuardExpr::FunctionCall { ref name, .. } => {
                        assert_eq!(name, "table_exists")
                    }
                    ref other => panic!("Expected FunctionCall on right, got {other:?}"),
                }
            }
            other => panic!("Expected Or, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_deeply_nested_parentheses() {
        let expr = parse("((table_exists(\"a\")))").unwrap();
        match expr {
            GuardExpr::FunctionCall { ref name, .. } => assert_eq!(name, "table_exists"),
            other => panic!("Expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_comparison_less_than() {
        let expr = parse("row_count(\"users\") < 1000").unwrap();
        match expr {
            GuardExpr::Comparison { left, op, right } => {
                match *left {
                    GuardExpr::FunctionCall { ref name, .. } => assert_eq!(name, "row_count"),
                    ref other => panic!("Expected FunctionCall on left, got {other:?}"),
                }
                assert_eq!(op, ComparisonOp::Lt);
                assert_eq!(*right, GuardExpr::NumberLiteral(1000));
            }
            other => panic!("Expected Comparison, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_comparison_greater_than() {
        let expr = parse("row_count(\"orders\") > 0").unwrap();
        match expr {
            GuardExpr::Comparison { op, .. } => assert_eq!(op, ComparisonOp::Gt),
            other => panic!("Expected Comparison, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_comparison_le_ge() {
        let expr = parse("row_count(\"t\") <= 500").unwrap();
        match expr {
            GuardExpr::Comparison { op, .. } => assert_eq!(op, ComparisonOp::Le),
            other => panic!("Expected Comparison, got {other:?}"),
        }

        let expr = parse("row_count(\"t\") >= 10").unwrap();
        match expr {
            GuardExpr::Comparison { op, .. } => assert_eq!(op, ComparisonOp::Ge),
            other => panic!("Expected Comparison, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_error_empty() {
        let result = parse("");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("empty expression"), "got: {err}");
    }

    #[test]
    fn test_parse_error_unterminated_string() {
        let result = parse("table_exists(\"users)");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unterminated string"), "got: {err}");
    }

    #[test]
    fn test_parse_error_unexpected_token() {
        let result = parse("AND");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_missing_closing_paren() {
        let result = parse("table_exists(\"users\"");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expected ')'"), "got: {err}");
    }

    #[test]
    fn test_parse_error_trailing_tokens() {
        let result = parse("table_exists(\"users\") table_exists(\"orders\")");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unexpected"), "got: {err}");
    }

    #[test]
    fn test_parse_error_unexpected_character() {
        let result = parse("table_exists(\"users\") @ foo");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unexpected character"), "got: {err}");
    }

    #[test]
    fn test_parse_complex_expression() {
        // (table_exists("users") AND NOT column_exists("users", "deleted_at"))
        //   OR (enum_exists("status") AND row_count("users") < 10000)
        let input = "(table_exists(\"users\") AND NOT column_exists(\"users\", \"deleted_at\")) \
                      OR (enum_exists(\"status\") AND row_count(\"users\") < 10000)";
        let expr = parse(input).unwrap();
        match expr {
            GuardExpr::Or(left, right) => {
                // Left: AND with NOT
                match *left {
                    GuardExpr::And(ref a, ref b) => {
                        match **a {
                            GuardExpr::FunctionCall { ref name, .. } => {
                                assert_eq!(name, "table_exists")
                            }
                            ref other => panic!("Expected FunctionCall, got {other:?}"),
                        }
                        match **b {
                            GuardExpr::Not(ref inner) => match **inner {
                                GuardExpr::FunctionCall { ref name, .. } => {
                                    assert_eq!(name, "column_exists")
                                }
                                ref other => panic!("Expected FunctionCall, got {other:?}"),
                            },
                            ref other => panic!("Expected Not, got {other:?}"),
                        }
                    }
                    ref other => panic!("Expected And, got {other:?}"),
                }
                // Right: AND with comparison
                match *right {
                    GuardExpr::And(ref a, ref b) => {
                        match **a {
                            GuardExpr::FunctionCall { ref name, .. } => {
                                assert_eq!(name, "enum_exists")
                            }
                            ref other => panic!("Expected FunctionCall, got {other:?}"),
                        }
                        match **b {
                            GuardExpr::Comparison {
                                ref op, ref right, ..
                            } => {
                                assert_eq!(*op, ComparisonOp::Lt);
                                assert_eq!(**right, GuardExpr::NumberLiteral(10000));
                            }
                            ref other => panic!("Expected Comparison, got {other:?}"),
                        }
                    }
                    ref other => panic!("Expected And, got {other:?}"),
                }
            }
            other => panic!("Expected Or, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_and_or_precedence() {
        // AND binds tighter than OR:
        // a OR b AND c  =>  a OR (b AND c)
        let expr =
            parse("table_exists(\"a\") OR table_exists(\"b\") AND table_exists(\"c\")").unwrap();
        match expr {
            GuardExpr::Or(left, right) => {
                match *left {
                    GuardExpr::FunctionCall { ref name, .. } => assert_eq!(name, "table_exists"),
                    ref other => panic!("Expected FunctionCall, got {other:?}"),
                }
                match *right {
                    GuardExpr::And(_, _) => {} // correct: AND grouped first
                    ref other => panic!("Expected And on right, got {other:?}"),
                }
            }
            other => panic!("Expected Or, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_chained_and() {
        let expr =
            parse("table_exists(\"a\") AND table_exists(\"b\") AND table_exists(\"c\")").unwrap();
        // Should be left-associative: (a AND b) AND c
        match expr {
            GuardExpr::And(left, right) => {
                match *left {
                    GuardExpr::And(_, _) => {} // left is itself an AND
                    ref other => panic!("Expected And on left (left-assoc), got {other:?}"),
                }
                match *right {
                    GuardExpr::FunctionCall { ref name, .. } => assert_eq!(name, "table_exists"),
                    ref other => panic!("Expected FunctionCall on right, got {other:?}"),
                }
            }
            other => panic!("Expected And, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_sql_function() {
        let expr = parse("sql(\"SELECT true\")").unwrap();
        match expr {
            GuardExpr::FunctionCall { name, args } => {
                assert_eq!(name, "sql");
                assert_eq!(args.len(), 1);
                assert_eq!(args[0], GuardExpr::StringLiteral("SELECT true".to_string()));
            }
            other => panic!("Expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_not_with_parentheses() {
        let expr = parse("NOT (table_exists(\"a\") OR table_exists(\"b\"))").unwrap();
        match expr {
            GuardExpr::Not(inner) => match *inner {
                GuardExpr::Or(_, _) => {} // correct
                ref other => panic!("Expected Or inside Not, got {other:?}"),
            },
            other => panic!("Expected Not, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_boolean_literals() {
        let expr = parse("true").unwrap();
        assert_eq!(expr, GuardExpr::BoolLiteral(true));

        let expr = parse("false").unwrap();
        assert_eq!(expr, GuardExpr::BoolLiteral(false));
    }

    #[test]
    fn test_tokenize_all_operators() {
        let tokens = tokenize("< > <= >= AND OR NOT ( ) ,").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Lt,
                Token::Gt,
                Token::Le,
                Token::Ge,
                Token::And,
                Token::Or,
                Token::Not,
                Token::LParen,
                Token::RParen,
                Token::Comma,
            ]
        );
    }

    #[test]
    fn test_builtin_sql_table_exists() {
        let (sql, params, is_bool) =
            builtin_sql("table_exists", &["users".to_string()], "public").unwrap();
        assert!(is_bool);
        assert!(sql.contains("information_schema.tables"));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
        assert_eq!(params, vec!["public", "users"]);
    }

    #[test]
    fn test_builtin_sql_column_exists() {
        let (sql, params, is_bool) = builtin_sql(
            "column_exists",
            &["users".to_string(), "email".to_string()],
            "public",
        )
        .unwrap();
        assert!(is_bool);
        assert!(sql.contains("information_schema.columns"));
        assert!(sql.contains("$3"));
        assert_eq!(params, vec!["public", "users", "email"]);
    }

    #[test]
    fn test_builtin_sql_row_count() {
        let (sql, params, is_bool) =
            builtin_sql("row_count", &["users".to_string()], "public").unwrap();
        assert!(!is_bool);
        assert!(sql.contains("pg_stat_user_tables"));
        assert!(sql.contains("n_live_tup"));
        assert_eq!(params, vec!["public", "users"]);
    }

    #[test]
    fn test_builtin_sql_unknown_function() {
        let result = builtin_sql("unknown_fn", &[], "public");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown function"), "got: {err}");
    }

    #[test]
    fn test_builtin_sql_wrong_arg_count() {
        let result = builtin_sql("table_exists", &[], "public");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expects 1 argument"), "got: {err}");
    }

    #[test]
    fn test_parse_depth_limit() {
        // Build a deeply nested expression: NOT NOT NOT ... NOT true
        let mut expr = String::new();
        for _ in 0..100 {
            expr.push_str("NOT ");
        }
        expr.push_str("true");
        let result = parse(&expr);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("maximum nesting depth exceeded"), "got: {err}");
    }

    #[test]
    fn test_builtin_sql_column_type() {
        let (sql, params, is_bool) = builtin_sql(
            "column_type",
            &[
                "users".to_string(),
                "age".to_string(),
                "integer".to_string(),
            ],
            "myschema",
        )
        .unwrap();
        assert!(is_bool);
        assert!(sql.contains("data_type = $4"));
        assert_eq!(params, vec!["myschema", "users", "age", "integer"]);
    }

    #[test]
    fn test_builtin_sql_column_nullable() {
        let (sql, params, is_bool) = builtin_sql(
            "column_nullable",
            &["users".to_string(), "name".to_string()],
            "public",
        )
        .unwrap();
        assert!(is_bool);
        assert!(sql.contains("is_nullable = 'YES'"));
        assert_eq!(params, vec!["public", "users", "name"]);
    }

    #[test]
    fn test_builtin_sql_index_exists() {
        let (sql, params, is_bool) =
            builtin_sql("index_exists", &["idx_users_email".to_string()], "public").unwrap();
        assert!(is_bool);
        assert!(sql.contains("pg_indexes"));
        assert!(sql.contains("$2"));
        assert_eq!(params, vec!["public", "idx_users_email"]);
    }

    #[test]
    fn test_builtin_sql_constraint_exists() {
        let (sql, params, is_bool) = builtin_sql(
            "constraint_exists",
            &["users".to_string(), "users_pkey".to_string()],
            "public",
        )
        .unwrap();
        assert!(is_bool);
        assert!(sql.contains("table_constraints"));
        assert!(sql.contains("$3"));
        assert_eq!(params, vec!["public", "users", "users_pkey"]);
    }

    #[test]
    fn test_builtin_sql_function_exists() {
        let (sql, params, is_bool) =
            builtin_sql("function_exists", &["my_func".to_string()], "public").unwrap();
        assert!(is_bool);
        assert!(sql.contains("pg_proc"));
        assert!(sql.contains("pg_namespace"));
        assert!(sql.contains("$2"));
        assert_eq!(params, vec!["public", "my_func"]);
    }

    #[test]
    fn test_builtin_sql_enum_exists() {
        let (sql, params, is_bool) =
            builtin_sql("enum_exists", &["status_type".to_string()], "public").unwrap();
        assert!(is_bool);
        assert!(sql.contains("pg_type"));
        assert!(sql.contains("typtype = 'e'"));
        assert!(sql.contains("$2"));
        assert_eq!(params, vec!["public", "status_type"]);
    }

    #[test]
    fn test_builtin_sql_custom_sql() {
        let (sql, params, is_bool) = builtin_sql(
            "sql",
            &["SELECT count(*) = 0 FROM old_table".to_string()],
            "public",
        )
        .unwrap();
        assert!(is_bool);
        assert_eq!(sql, "SELECT count(*) = 0 FROM old_table");
        assert!(params.is_empty());
    }

    #[test]
    fn test_builtin_sql_params_order_table_exists() {
        let (sql, params, is_bool) =
            builtin_sql("table_exists", &["users".to_string()], "myschema").unwrap();
        assert!(is_bool);
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], "myschema");
        assert_eq!(params[1], "users");
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
    }

    #[test]
    fn test_builtin_sql_sql_function_empty_params() {
        let (sql, params, is_bool) =
            builtin_sql("sql", &["SELECT 1".to_string()], "public").unwrap();
        assert!(is_bool);
        assert!(params.is_empty());
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn test_parse_empty_function_args() {
        // table_exists() with no args should be parsed OK but builtin_sql should reject it
        let expr = parse("table_exists()").unwrap();
        match expr {
            GuardExpr::FunctionCall { name, args } => {
                assert_eq!(name, "table_exists");
                assert!(args.is_empty());
            }
            other => panic!("Expected FunctionCall, got {other:?}"),
        }
    }
}
