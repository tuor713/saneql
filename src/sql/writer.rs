use std::collections::HashMap;
use std::rc::Rc;
use crate::algebra::IU;
use crate::infra::schema::{Type, TypeBase};

/// Accumulates a SQL string and assigns stable names to IUs.
pub struct SqlWriter {
    buf:      String,
    iu_names: HashMap<u64, String>, // iu.id → "v_N"
}

impl SqlWriter {
    pub fn new() -> Self {
        SqlWriter { buf: String::new(), iu_names: HashMap::new() }
    }

    pub fn write(&mut self, s: &str) {
        self.buf.push_str(s);
    }

    /// Assign a stable `v_N` name to the IU on first encounter, then write it.
    pub fn write_iu(&mut self, iu: &Rc<IU>) {
        let next = self.iu_names.len() + 1;
        let name = self.iu_names
            .entry(iu.id)
            .or_insert_with(|| format!("v_{next}"))
            .clone();
        self.buf.push_str(&name);
    }

    /// Write `"identifier"` with `""` escaping.
    pub fn write_identifier(&mut self, id: &str) {
        self.buf.push('"');
        for c in id.chars() {
            if c == '"' { self.buf.push('"'); }
            self.buf.push(c);
        }
        self.buf.push('"');
    }

    /// Write `'string'` with `''` escaping.
    pub fn write_string(&mut self, s: &str) {
        self.buf.push('\'');
        for c in s.chars() {
            if c == '\'' { self.buf.push('\''); }
            self.buf.push(c);
        }
        self.buf.push('\'');
    }

    /// Write an SQL type name.
    pub fn write_type(&mut self, t: Type) {
        let s = match t.base {
            TypeBase::Unknown           => "unknown".to_string(),
            TypeBase::Bool              => "boolean".to_string(),
            TypeBase::Integer           => "integer".to_string(),
            TypeBase::Bigint            => "bigint".to_string(),
            TypeBase::Double            => "double".to_string(),
            TypeBase::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
            TypeBase::Char    { len }   => format!("char({len})"),
            TypeBase::Varchar { len }   => format!("varchar({len})"),
            TypeBase::Text              => "text".to_string(),
            TypeBase::Date              => "date".to_string(),
            TypeBase::Timestamp         => "timestamp".to_string(),
            TypeBase::Interval          => "interval".to_string(),
        };
        self.buf.push_str(&s);
    }

    pub fn get_result(self) -> String { self.buf }
}
