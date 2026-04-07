pub mod infra;
pub mod algebra;
pub mod sql;
pub mod semana;
pub mod parser;
pub mod wasm;

pub use parser::parse;

use infra::schema::{Column, SchemaProvider, Table, parse_type_str};
use semana::SemanticAnalysis;
use algebra::Op;
use sql::SqlWriter;

/// Internal: compile a parsed query using any [`SchemaProvider`].
pub(crate) fn compile_inner(input: &str, schema: Box<dyn SchemaProvider>) -> Result<String, String> {
    let ast = parser::parse(input)?;
    let mut semana = SemanticAnalysis::with_provider(schema);
    let result = semana.analyze_query(&ast)?;

    // Scalar result: emit as `select EXPR`
    if result.is_scalar() {
        let expr = result.expr();
        let mut out = SqlWriter::new();
        out.write("select ");
        expr.generate(&mut out);
        return Ok(out.get_result());
    }

    let (op, binding) = result.into_parts();

    let mut out = SqlWriter::new();

    let write_col_list = |out: &mut SqlWriter, cols: &[semana::ColumnEntry]| {
        for (i, col) in cols.iter().enumerate() {
            if i > 0 { out.write(", "); }
            out.write_iu(&col.iu);
            // Add AS "name" alias when the name is non-empty and not a gensym
            if !col.name.is_empty() && !col.name.starts_with(' ') {
                out.write(" as ");
                out.write_identifier(&col.name);
            }
        }
    };

    // Unwrap a top-level Sort so we can emit
    // select cols from (inner) s order by ...
    match *op {
        Op::Sort { input, order, limit, offset } => {
            out.write("select ");
            write_col_list(&mut out, &binding.columns);
            out.write(" from ");
            input.generate(&mut out);
            out.write(" s");
            if !order.is_empty() {
                out.write(" order by ");
                for (i, o) in order.iter().enumerate() {
                    if i > 0 { out.write(", "); }
                    o.value.generate(&mut out);
                    if o.descending { out.write(" desc"); }
                }
            }
            if let Some(l) = limit  { out.write(" limit ");  out.write(&l.to_string()); }
            if let Some(o) = offset { out.write(" offset "); out.write(&o.to_string()); }
        }
        other => {
            out.write("select ");
            write_col_list(&mut out, &binding.columns);
            out.write(" from ");
            other.generate(&mut out);
            out.write(" s");
        }
    }

    Ok(out.get_result())
}

/// Compile a SaneQL query using a caller-supplied schema callback.
///
/// `get_columns(table_name)` is called lazily for each table referenced in
/// the query.  It should return a list of `(column_name, type_string)` pairs,
/// or `None` if the table does not exist.  `table_name` is the name exactly
/// as written in the query (e.g. `"catalog.schema.orders"`).
pub fn compile_with_schema<F>(input: &str, get_columns: F) -> Result<String, String>
where
    F: Fn(&str) -> Option<Vec<(String, String)>> + 'static,
{
    struct CallbackProvider<F>(F);
    impl<F: Fn(&str) -> Option<Vec<(String, String)>>> SchemaProvider for CallbackProvider<F> {
        fn lookup_table(&self, name: &str) -> Option<Table> {
            let cols = (self.0)(name)?;
            let columns = cols
                .into_iter()
                .map(|(col_name, type_str)| {
                    let typ = parse_type_str(&type_str).unwrap_or_else(infra::schema::Type::unknown);
                    Column { name: col_name, typ }
                })
                .collect();
            Some(Table { columns })
        }
    }

    compile_inner(input, Box::new(CallbackProvider(get_columns)))
}
