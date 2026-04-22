pub mod infra;
pub mod algebra;
pub mod sql;
pub mod semana;
pub mod parser;
pub mod wasm;

pub use parser::parse;

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use infra::schema::{Column, SchemaProvider, Table, parse_type_str};
use semana::SemanticAnalysis;
use algebra::Op;
use std::rc::Rc;
use sql::SqlWriter;

/// Prune the Op tree top-down, keeping only columns needed by the consumer.
/// `needed` is the set of IU ids required from this Op's output.
fn prune_op(op: Box<Op>, needed: &HashSet<u64>) -> Box<Op> {
    match *op {
        Op::TableScan { parts, columns } => {
            let pruned = columns.into_iter()
                .filter(|(_, iu)| needed.contains(&iu.id))
                .collect();
            Box::new(Op::TableScan { parts, columns: pruned })
        }

        Op::Select { input, condition } => {
            let mut from_input = needed.clone();
            condition.collect_iu_ids(&mut from_input);
            Box::new(Op::Select {
                input: prune_op(input, &from_input),
                condition,
            })
        }

        Op::Map { input, computations, .. } => {
            let (kept_comps, _): (Vec<_>, Vec<_>) = computations
                .into_iter()
                .partition(|c| c.iu.as_ref().map_or(false, |iu| needed.contains(&iu.id)));

            let comp_ids: HashSet<u64> = kept_comps.iter()
                .filter_map(|c| c.iu.as_ref())
                .map(|iu| iu.id)
                .collect();

            let mut from_input: HashSet<u64> = needed.difference(&comp_ids).cloned().collect();
            for c in &kept_comps {
                c.value.collect_iu_ids(&mut from_input);
            }

            let input_produced = input.produced_ius();
            let mut pass_throughs: Vec<_> = needed.difference(&comp_ids)
                .filter_map(|id| input_produced.get(id).map(Rc::clone))
                .collect();
            pass_throughs.sort_by_key(|iu| iu.id);

            Box::new(Op::Map {
                input: prune_op(input, &from_input),
                computations: kept_comps,
                pass_throughs: Some(pass_throughs),
            })
        }

        Op::Sort { input, order, limit, offset } => {
            let mut from_input = needed.clone();
            for o in &order { o.value.collect_iu_ids(&mut from_input); }
            Box::new(Op::Sort {
                input: prune_op(input, &from_input),
                order,
                limit,
                offset,
            })
        }

        Op::GroupBy { input, group_by, aggregates } => {
            let mut from_input = HashSet::new();
            for g in &group_by { g.value.collect_iu_ids(&mut from_input); }
            for a in &aggregates {
                if let Some(v) = &a.value { v.collect_iu_ids(&mut from_input); }
                for p in &a.params { p.collect_iu_ids(&mut from_input); }
            }
            Box::new(Op::GroupBy {
                input: prune_op(input, &from_input),
                group_by,
                aggregates,
            })
        }

        other => Box::new(other),
    }
}

/// Internal: compile a parsed query using any [`SchemaProvider`].
pub(crate) async fn compile_inner(input: &str, schema: Box<dyn SchemaProvider>) -> Result<String, String> {
    let ast = parser::parse(input)?;
    let mut semana = SemanticAnalysis::with_provider(schema);
    let result = semana.analyze_query(&ast).await?;

    // Scalar result: emit as `select EXPR`
    if result.is_scalar() {
        let expr = result.expr();
        let mut out = SqlWriter::new();
        out.write("select ");
        expr.generate(&mut out);
        return Ok(out.get_result());
    }

    let (op, binding) = result.into_parts();

    let needed: HashSet<u64> = binding.columns.iter().map(|c| c.iu.id).collect();
    let op = prune_op(op, &needed);

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
pub async fn compile_with_schema<F>(input: &str, get_columns: F) -> Result<String, String>
where
    F: Fn(&str) -> Option<Vec<(String, String)>> + 'static,
{
    struct CallbackProvider<F>(F);
    impl<F: Fn(&str) -> Option<Vec<(String, String)>> + 'static> SchemaProvider for CallbackProvider<F> {
        fn lookup_table<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Option<Table>> + 'a>> {
            Box::pin(async move {
                let cols = (self.0)(name)?;
                let columns = cols
                    .into_iter()
                    .map(|(col_name, type_str)| {
                        let typ = parse_type_str(&type_str).unwrap_or_else(infra::schema::Type::unknown);
                        Column { name: col_name, typ }
                    })
                    .collect();
                Some(Table { columns })
            })
        }
    }

    compile_inner(input, Box::new(CallbackProvider(get_columns))).await
}
