use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

// ── SchemaProvider trait ──────────────────────────────────────────────────────

/// Lazy, on-demand schema lookup.  The table name may be a qualified
/// `catalog.schema.table` name exactly as it appears in the query.
///
/// `lookup_table` returns a boxed future so the trait remains object-safe
/// (`Box<dyn SchemaProvider>`) while still supporting async implementations.
pub trait SchemaProvider {
    fn lookup_table<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Option<Table>> + 'a>>;
}


/// The "kind" of an SQL type (without nullability).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeBase {
    Unknown,
    Bool,
    Integer,
    Double,
    Decimal { precision: u32, scale: u32 },
    Char { len: u32 },
    Varchar { len: u32 },
    Text,
    Date,
    Timestamp,
    Interval,
}

/// An SQL type with nullability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Type {
    pub base: TypeBase,
    pub nullable: bool,
}

impl Type {
    // ── constructors ──────────────────────────────────────────────────────
    pub fn unknown() -> Self {
        Self {
            base: TypeBase::Unknown,
            nullable: false,
        }
    }
    pub fn bool_() -> Self {
        Self {
            base: TypeBase::Bool,
            nullable: false,
        }
    }
    pub fn integer() -> Self {
        Self {
            base: TypeBase::Integer,
            nullable: false,
        }
    }
    pub fn decimal(precision: u32, scale: u32) -> Self {
        Self {
            base: TypeBase::Decimal { precision, scale },
            nullable: false,
        }
    }
    pub fn char_(len: u32) -> Self {
        Self {
            base: TypeBase::Char { len },
            nullable: false,
        }
    }
    pub fn varchar(len: u32) -> Self {
        Self {
            base: TypeBase::Varchar { len },
            nullable: false,
        }
    }
    pub fn text() -> Self {
        Self {
            base: TypeBase::Text,
            nullable: false,
        }
    }
    pub fn date() -> Self {
        Self {
            base: TypeBase::Date,
            nullable: false,
        }
    }
    pub fn timestamp() -> Self {
        Self {
            base: TypeBase::Timestamp,
            nullable: false,
        }
    }
    pub fn interval() -> Self {
        Self {
            base: TypeBase::Interval,
            nullable: false,
        }
    }
    pub fn double() -> Self {
        Self {
            base: TypeBase::Double,
            nullable: false,
        }
    }

    // ── nullability helpers ───────────────────────────────────────────────
    pub fn as_nullable(self) -> Self {
        Self {
            nullable: true,
            ..self
        }
    }
    pub fn with_nullable(self, nullable: bool) -> Self {
        Self { nullable, ..self }
    }
    pub fn is_nullable(self) -> bool {
        self.nullable
    }

    // ── kind helpers ──────────────────────────────────────────────────────
    pub fn is_numeric(self) -> bool {
        matches!(self.base, TypeBase::Integer | TypeBase::Double | TypeBase::Decimal { .. })
    }
    pub fn is_temporal(self) -> bool {
        matches!(self.base, TypeBase::Date | TypeBase::Timestamp)
    }
    pub fn is_string(self) -> bool {
        matches!(
            self.base,
            TypeBase::Char { .. } | TypeBase::Varchar { .. } | TypeBase::Text
        )
    }
    pub fn name(self) -> &'static str {
        match self.base {
            TypeBase::Unknown => "unknown",
            TypeBase::Bool => "boolean",
            TypeBase::Integer => "integer",
            TypeBase::Double => "double",
            TypeBase::Decimal { .. } => "decimal",
            TypeBase::Char { .. } => "char",
            TypeBase::Varchar { .. } => "varchar",
            TypeBase::Text => "text",
            TypeBase::Date => "date",
            TypeBase::Timestamp => "timestamp",
            TypeBase::Interval => "interval",
        }
    }
}

// ── Schema ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub typ: Type,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Default)]
pub struct Schema {
    tables: HashMap<String, Table>,
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, name: &str, cols: &[(&str, Type)]) {
        self.create(name, cols);
    }

    fn create(&mut self, name: &str, cols: &[(&str, Type)]) {
        let columns = cols
            .iter()
            .map(|(n, t)| Column {
                name: n.to_string(),
                typ: *t,
            })
            .collect();
        self.tables.insert(name.to_string(), Table { columns });
    }

    pub fn lookup_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
}

impl SchemaProvider for Schema {
    fn lookup_table<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Option<Table>> + 'a>> {
        Box::pin(async move { self.tables.get(name).cloned() })
    }
}

// ── Type-string parser ────────────────────────────────────────────────────────

/// Parse a Trino / SQL type string such as `"integer"`, `"varchar(25)"`,
/// `"decimal(12,2)"`, `"char(10)"` into a [`Type`].
///
/// Returns `None` for unrecognised type strings.
pub fn parse_type_str(s: &str) -> Option<Type> {
    let s = s.trim();
    let lower = s.to_ascii_lowercase();
    let lower = lower.trim();

    // Types with parenthesised arguments
    if let Some(rest) = lower.strip_suffix(')') {
        if let Some(idx) = rest.find('(') {
            let name = rest[..idx].trim();
            let args = rest[idx + 1..].trim();
            match name {
                "varchar" | "character varying" => {
                    let n: u32 = args.parse().ok()?;
                    return Some(Type::varchar(n));
                }
                "char" | "character" => {
                    let n: u32 = args.parse().ok()?;
                    return Some(Type::char_(n));
                }
                "decimal" | "numeric" => {
                    let mut parts = args.splitn(2, ',');
                    let p: u32 = parts.next()?.trim().parse().ok()?;
                    let s: u32 = parts.next()?.trim().parse().ok()?;
                    return Some(Type::decimal(p, s));
                }
                _ => {}
            }
        }
    }

    // Plain keyword types
    Some(match lower {
        "integer" | "int" | "int4" | "bigint" | "int8" | "smallint" | "int2"
        | "tinyint" => Type::integer(),
        "boolean" | "bool" => Type::bool_(),
        "text" | "string" => Type::text(),
        "varchar" | "character varying" => Type::text(), // unbounded
        "date" => Type::date(),
        "timestamp" | "timestamp with time zone" => Type::timestamp(),
        "interval" | "interval year to month" | "interval day to second" => Type::interval(),
        "double" | "double precision" | "real" | "float" | "float4" | "float8" => {
            Type::double()
        }
        _ => return None,
    })
}
