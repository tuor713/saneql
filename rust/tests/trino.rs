/// Tests for SaneQL → Trino SQL conversion.
///
/// Each test calls `compile_with_schema` with an inline schema callback and
/// asserts the exact SQL string produced.

extern crate pollster;

fn schema(tables: &'static [(&'static str, &'static [(&'static str, &'static str)])])
    -> impl Fn(&str) -> Option<Vec<(String, String)>>
{
    move |table_name: &str| {
        tables.iter().find(|(name, _)| *name == table_name).map(|(_, cols)| {
            cols.iter().map(|(c, t)| (c.to_string(), t.to_string())).collect()
        })
    }
}

fn compile(query: &str, tables: &'static [(&'static str, &'static [(&'static str, &'static str)])]) -> String {
    pollster::block_on(saneql::compile_with_schema(query, schema(tables)))
        .unwrap_or_else(|e| panic!("compile error: {e}"))
}

// ---------------------------------------------------------------------------
// Qualified table names
// ---------------------------------------------------------------------------

/// Three-part name where no component contains dots.
#[test]
fn triple_part_table_name() {
    const TABLES: &[(&str, &[(&str, &str)])] = &[(
        "system.runtime.queries",
        &[
            ("query_id", "varchar"),
            ("state",    "varchar"),
            ("user",     "varchar"),
        ],
    )];

    let sql = compile("system.runtime.queries", TABLES);

    assert_eq!(
        sql,
        r#"select v_1 as "query_id", v_2 as "state", v_3 as "user" from (select "query_id" as v_1, "state" as v_2, "user" as v_3 from "system"."runtime"."queries") s"#
    );
}

/// Three-part name where the table component itself contains dots
/// (e.g. a Kafka topic name used via the Trino Kafka connector).
/// SaneQL source: `kafka.default."my.topic.name"`
/// Expected SQL:  `... from "kafka"."default"."my.topic.name" ...`
#[test]
fn dotted_topic_name() {
    const TABLES: &[(&str, &[(&str, &str)])] = &[(
        // The schema callback key is the parts joined with '.'.
        // Since catalog/schema never contain dots in Trino, the user can
        // reliably split on the first two '.' to recover the three components.
        "kafka.default.my.topic.name",
        &[
            ("key",       "varchar"),
            ("message",   "varchar"),
            ("partition", "integer"),
        ],
    )];

    let sql = compile(r#"kafka.default."my.topic.name""#, TABLES);

    assert_eq!(
        sql,
        r#"select v_1 as "key", v_2 as "message", v_3 as "partition" from (select "key" as v_1, "message" as v_2, "partition" as v_3 from "kafka"."default"."my.topic.name") s"#
    );
}
