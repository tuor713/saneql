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

// ---------------------------------------------------------------------------
// limit()
// ---------------------------------------------------------------------------

const NATION: &[(&str, &[(&str, &str)])] = &[(
    "nation",
    &[
        ("n_nationkey", "integer"),
        ("n_name",      "varchar"),
        ("n_regionkey", "integer"),
        ("n_comment",   "varchar"),
    ],
)];

/// `table.limit(n)` wraps the input in a bare LIMIT without ORDER BY.
#[test]
fn limit_simple() {
    let sql = compile("nation.limit(100)", NATION);
    assert_eq!(
        sql,
        r#"select v_1 as "n_nationkey", v_2 as "n_name", v_3 as "n_regionkey", v_4 as "n_comment" from (select "n_nationkey" as v_1, "n_name" as v_2, "n_regionkey" as v_3, "n_comment" as v_4 from "nation") s limit 100"#
    );
}

/// `limit()` can be chained after a filter.
#[test]
fn limit_after_filter() {
    let sql = compile("nation.filter(n_regionkey = 1).limit(10)", NATION);
    assert_eq!(
        sql,
        r#"select v_1 as "n_nationkey", v_2 as "n_name", v_3 as "n_regionkey", v_4 as "n_comment" from (select * from (select "n_nationkey" as v_1, "n_name" as v_2, "n_regionkey" as v_3, "n_comment" as v_4 from "nation") s where v_3 = cast('1' as integer)) s limit 10"#
    );
}

// ---------------------------------------------------------------------------
// Math functions
// ---------------------------------------------------------------------------

const NUMS: &[(&str, &[(&str, &str)])] = &[(
    "nums",
    &[
        ("i", "integer"),
        ("d", "double"),
    ],
)];

/// abs, ceil, floor, sign, truncate preserve the input type.
#[test]
fn math_preserve_type() {
    let sql = compile("nums.map({a:=abs(i), b:=ceil(i), c:=floor(i), d:=sign(i), e:=truncate(i)})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b", v_5 as "c", v_6 as "d", v_7 as "e" from (select *, abs(v_1) as v_3, ceil(v_1) as v_4, floor(v_1) as v_5, sign(v_1) as v_6, truncate(v_1) as v_7 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// round with one and two arguments.
#[test]
fn math_round() {
    let sql = compile("nums.map({a:=round(d), b:=round(d, 2)})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b" from (select *, round(v_2) as v_3, round(v_2, cast('2' as integer)) as v_4 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// sqrt, ln, log2, log10, exp, cbrt return double.
#[test]
fn math_returns_double() {
    let sql = compile("nums.map({a:=sqrt(i), b:=ln(i), c:=log2(i), d:=log10(i), e:=exp(i), f:=cbrt(i)})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b", v_5 as "c", v_6 as "d", v_7 as "e", v_8 as "f" from (select *, sqrt(v_1) as v_3, ln(v_1) as v_4, log2(v_1) as v_5, log10(v_1) as v_6, exp(v_1) as v_7, cbrt(v_1) as v_8 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// Trig functions return double.
#[test]
fn math_trig() {
    let sql = compile("nums.map({a:=sin(d), b:=cos(d), c:=tan(d), x:=asin(d), y:=acos(d), z:=atan(d)})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b", v_5 as "c", v_6 as "x", v_7 as "y", v_8 as "z" from (select *, sin(v_2) as v_3, cos(v_2) as v_4, tan(v_2) as v_5, asin(v_2) as v_6, acos(v_2) as v_7, atan(v_2) as v_8 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// degrees, radians conversions.
#[test]
fn math_angle_conversion() {
    let sql = compile("nums.map({a:=degrees(d), b:=radians(d)})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b" from (select *, degrees(v_2) as v_3, radians(v_2) as v_4 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// Zero-argument constants.
#[test]
fn math_constants() {
    let sql = compile("nums.map({a:=pi(), b:=e(), c:=infinity(), d:=nan()})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b", v_5 as "c", v_6 as "d" from (select *, pi() as v_3, e() as v_4, infinity() as v_5, nan() as v_6 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
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
