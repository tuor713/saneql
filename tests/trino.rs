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
        r#"select v_1 as "n_nationkey", v_2 as "n_name", v_3 as "n_regionkey", v_4 as "n_comment" from (select * from (select "n_nationkey" as v_1, "n_name" as v_2, "n_regionkey" as v_3, "n_comment" as v_4 from "nation") s where v_3 = 1) s limit 10"#
    );
}

// ---------------------------------------------------------------------------
// map() behaviour
// ---------------------------------------------------------------------------

/// map() must preserve columns that are not referenced in the map expressions.
#[test]
fn map_preserves_unreferenced_columns() {
    let sql = compile("table({a:=1, b:=2}).map({c:=a+1})", &[]);
    // b must appear in the output even though it is not referenced in the map
    assert!(
        sql.contains(r#"as "b""#),
        "column 'b' missing from map output; got: {sql}"
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
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b", v_5 as "c", v_6 as "d", v_7 as "e" from (select v_1, v_2, abs(v_1) as v_3, ceil(v_1) as v_4, floor(v_1) as v_5, sign(v_1) as v_6, truncate(v_1) as v_7 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// round with one and two arguments.
#[test]
fn math_round() {
    let sql = compile("nums.map({a:=round(d), b:=round(d, 2)})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b" from (select v_1, v_2, round(v_2) as v_3, round(v_2, 2) as v_4 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// sqrt, ln, log2, log10, exp, cbrt return double.
#[test]
fn math_returns_double() {
    let sql = compile("nums.map({a:=sqrt(i), b:=ln(i), c:=log2(i), d:=log10(i), e:=exp(i), f:=cbrt(i)})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b", v_5 as "c", v_6 as "d", v_7 as "e", v_8 as "f" from (select v_1, v_2, sqrt(v_1) as v_3, ln(v_1) as v_4, log2(v_1) as v_5, log10(v_1) as v_6, exp(v_1) as v_7, cbrt(v_1) as v_8 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// Trig functions return double.
#[test]
fn math_trig() {
    let sql = compile("nums.map({a:=sin(d), b:=cos(d), c:=tan(d), x:=asin(d), y:=acos(d), z:=atan(d)})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b", v_5 as "c", v_6 as "x", v_7 as "y", v_8 as "z" from (select v_1, v_2, sin(v_2) as v_3, cos(v_2) as v_4, tan(v_2) as v_5, asin(v_2) as v_6, acos(v_2) as v_7, atan(v_2) as v_8 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// degrees, radians conversions.
#[test]
fn math_angle_conversion() {
    let sql = compile("nums.map({a:=degrees(d), b:=radians(d)})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b" from (select v_1, v_2, degrees(v_2) as v_3, radians(v_2) as v_4 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

/// Zero-argument constants.
#[test]
fn math_constants() {
    let sql = compile("nums.map({a:=pi(), b:=e(), c:=infinity(), d:=nan()})", NUMS);
    assert_eq!(
        sql,
        r#"select v_1 as "i", v_2 as "d", v_3 as "a", v_4 as "b", v_5 as "c", v_6 as "d" from (select v_1, v_2, pi() as v_3, e() as v_4, infinity() as v_5, nan() as v_6 from (select "i" as v_1, "d" as v_2 from "nums") s) s"#
    );
}

// ---------------------------------------------------------------------------
// Date / time functions
// ---------------------------------------------------------------------------

const EVENTS: &[(&str, &[(&str, &str)])] = &[(
    "events",
    &[
        ("ts",  "timestamp"),
        ("dt",  "date"),
        ("val", "double"),
    ],
)];

/// current_date, current_timestamp and now().
#[test]
fn datetime_current() {
    let sql = compile(
        "events.map({a:=current_date(), b:=current_timestamp(), c:=now()})",
        EVENTS,
    );
    assert_eq!(
        sql,
        r#"select v_1 as "ts", v_2 as "dt", v_3 as "val", v_4 as "a", v_5 as "b", v_6 as "c" from (select v_1, v_2, v_3, current_date as v_4, current_timestamp as v_5, now() as v_6 from (select "ts" as v_1, "dt" as v_2, "val" as v_3 from "events") s) s"#
    );
}

/// Extraction functions: year, month, day, hour, minute, second.
#[test]
fn datetime_extractors() {
    let sql = compile(
        "events.map({y:=year(ts), mo:=month(ts), d:=day(ts), h:=hour(ts), mi:=minute(ts), s:=second(ts)})",
        EVENTS,
    );
    assert_eq!(
        sql,
        r#"select v_1 as "ts", v_2 as "dt", v_3 as "val", v_4 as "y", v_5 as "mo", v_6 as "d", v_7 as "h", v_8 as "mi", v_9 as "s" from (select v_1, v_2, v_3, year(v_1) as v_4, month(v_1) as v_5, day(v_1) as v_6, hour(v_1) as v_7, minute(v_1) as v_8, second(v_1) as v_9 from (select "ts" as v_1, "dt" as v_2, "val" as v_3 from "events") s) s"#
    );
}

/// date_diff, date_add, date_trunc.
#[test]
fn datetime_arithmetic() {
    let sql = compile(
        r#"events.map({diff:=date_diff('day', dt, ts), added:=date_add('hour', 1, ts), trunc:=date_trunc('month', ts)})"#,
        EVENTS,
    );
    assert_eq!(
        sql,
        r#"select v_1 as "ts", v_2 as "dt", v_3 as "val", v_4 as "diff", v_5 as "added", v_6 as "trunc" from (select v_1, v_2, v_3, date_diff('day', v_2, v_1) as v_4, date_add('hour', 1, v_1) as v_5, date_trunc('month', v_1) as v_6 from (select "ts" as v_1, "dt" as v_2, "val" as v_3 from "events") s) s"#
    );
}

/// date_format, to_unixtime, from_unixtime, to_iso8601, from_iso8601_date.
#[test]
fn datetime_conversions() {
    let sql = compile(
        r#"events.map({fmt:=date_format(ts, '%Y-%m-%d'), unix:=to_unixtime(ts), back:=from_unixtime(val), iso:=to_iso8601(dt), d2:=from_iso8601_date('2024-01-01')})"#,
        EVENTS,
    );
    assert_eq!(
        sql,
        r#"select v_1 as "ts", v_2 as "dt", v_3 as "val", v_4 as "fmt", v_5 as "unix", v_6 as "back", v_7 as "iso", v_8 as "d2" from (select v_1, v_2, v_3, date_format(v_1, '%Y-%m-%d') as v_4, to_unixtime(v_1) as v_5, from_unixtime(v_3) as v_6, to_iso8601(v_2) as v_7, from_iso8601_date('2024-01-01') as v_8 from (select "ts" as v_1, "dt" as v_2, "val" as v_3 from "events") s) s"#
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

// ---------------------------------------------------------------------------
// Let-function parameters visible inside filters on qualified tables
// ---------------------------------------------------------------------------

/// Regression test: a scalar parameter passed to a let-function must be
/// visible inside a `.filter()` predicate when the table is referenced by a
/// qualified (dotted) name.  Previously `memory.default.risk` was scanned
/// with the root scope, so `d` was not found in the argument scope and the
/// analyzer reported "unknown table 'd'".
#[test]
fn let_param_visible_in_qualified_table_filter() {
    const TABLES: &[(&str, &[(&str, &str)])] = &[(
        "memory.default.risk",
        &[
            ("businessdate", "integer"),
            ("value",        "double"),
        ],
    )];

    let sql = compile(
        "let risk(d) := memory.default.risk.filter(businessdate = d),\nrisk(20250130).groupby({}, {c := count()})",
        TABLES,
    );

    assert_eq!(
        sql,
        r#"select v_1 as "c" from (select count(*) as v_1 from (select * from (select "businessdate" as v_2 from "memory"."default"."risk") s where v_2 = 20250130) s group by true) s"#
    );
}

// ---------------------------------------------------------------------------
// String functions
// ---------------------------------------------------------------------------

const STRS: &[(&str, &[(&str, &str)])] = &[(
    "strs",
    &[("s", "varchar"), ("t", "varchar"), ("n", "integer")],
)];

/// lower, upper, ltrim, rtrim, trim, reverse, soundex → varchar.
#[test]
fn string_single_arg_varchar() {
    let sql = compile(
        r#"strs.map({a:=lower(s), b:=upper(s), c:=ltrim(s), d:=rtrim(s), e:=trim(s), f:=reverse(s), g:=soundex(s)})"#,
        STRS,
    );
    assert_eq!(
        sql,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a", v_5 as "b", v_6 as "c", v_7 as "d", v_8 as "e", v_9 as "f", v_10 as "g" from (select v_1, v_2, v_3, lower(v_1) as v_4, upper(v_1) as v_5, ltrim(v_1) as v_6, rtrim(v_1) as v_7, trim(v_1) as v_8, reverse(v_1) as v_9, soundex(v_1) as v_10 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// length, codepoint → bigint.
#[test]
fn string_single_arg_bigint() {
    let sql = compile(r#"strs.map({a:=length(s), b:=codepoint(s)})"#, STRS);
    assert_eq!(
        sql,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a", v_5 as "b" from (select v_1, v_2, v_3, length(v_1) as v_4, codepoint(v_1) as v_5 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// chr(integer) → varchar.
#[test]
fn string_chr() {
    let sql = compile(r#"strs.map({a:=chr(n)})"#, STRS);
    assert_eq!(
        sql,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, chr(v_3) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// luhn_check(varchar) → boolean.
#[test]
fn string_luhn_check() {
    let sql = compile(r#"strs.map({a:=luhn_check(s)})"#, STRS);
    assert_eq!(
        sql,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, luhn_check(v_1) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// starts_with(string, substring) → boolean.
#[test]
fn string_starts_with() {
    let sql = compile(r#"strs.map({a:=starts_with(s, 'he')})"#, STRS);
    assert_eq!(
        sql,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, starts_with(v_1, 'he') as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// levenshtein_distance, hamming_distance → bigint.
#[test]
fn string_distance_functions() {
    let sql = compile(
        r#"strs.map({a:=levenshtein_distance(s, t), b:=hamming_distance(s, t)})"#,
        STRS,
    );
    assert_eq!(
        sql,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a", v_5 as "b" from (select v_1, v_2, v_3, levenshtein_distance(v_1, v_2) as v_4, hamming_distance(v_1, v_2) as v_5 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// strpos with 2 and 3 arguments → bigint.
#[test]
fn string_strpos() {
    let sql2 = compile(r#"strs.map({a:=strpos(s, t)})"#, STRS);
    assert_eq!(
        sql2,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, strpos(v_1, v_2) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
    let sql3 = compile(r#"strs.map({a:=strpos(s, t, 2)})"#, STRS);
    assert_eq!(
        sql3,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, strpos(v_1, v_2, 2) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// replace with 2 and 3 arguments → varchar.
#[test]
fn string_replace() {
    let sql2 = compile(r#"strs.map({a:=replace(s, t)})"#, STRS);
    assert_eq!(
        sql2,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, replace(v_1, v_2) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
    let sql3 = compile(r#"strs.map({a:=replace(s, t, 'x')})"#, STRS);
    assert_eq!(
        sql3,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, replace(v_1, v_2, 'x') as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// word_stem with 1 and 2 arguments → varchar.
#[test]
fn string_word_stem() {
    let sql1 = compile(r#"strs.map({a:=word_stem(s)})"#, STRS);
    assert_eq!(
        sql1,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, word_stem(v_1) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
    let sql2 = compile(r#"strs.map({a:=word_stem(s, 'en')})"#, STRS);
    assert_eq!(
        sql2,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, word_stem(v_1, 'en') as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// substring with 2 and 3 arguments → varchar.
#[test]
fn string_substring() {
    let sql2 = compile(r#"strs.map({a:=substring(s, 2)})"#, STRS);
    assert_eq!(
        sql2,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, substring(v_1, 2) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
    let sql3 = compile(r#"strs.map({a:=substring(s, 2, 3)})"#, STRS);
    assert_eq!(
        sql3,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, substring(v_1, 2, 3) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// lpad and rpad (3 args) → varchar.
#[test]
fn string_pad() {
    let lpad = compile(r#"strs.map({a:=lpad(s, 10, '0')})"#, STRS);
    assert_eq!(
        lpad,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, lpad(v_1, 10, '0') as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
    let rpad = compile(r#"strs.map({a:=rpad(s, 10, '0')})"#, STRS);
    assert_eq!(
        rpad,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, rpad(v_1, 10, '0') as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// translate(source, from, to) → varchar.
#[test]
fn string_translate() {
    let sql = compile(r#"strs.map({a:=translate(s, 'abc', 'xyz')})"#, STRS);
    assert_eq!(
        sql,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, translate(v_1, 'abc', 'xyz') as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// split_part(string, delimiter, index) → varchar.
#[test]
fn string_split_part() {
    let sql = compile(r#"strs.map({a:=split_part(s, ',', 1)})"#, STRS);
    assert_eq!(
        sql,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, split_part(v_1, ',', 1) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}

/// concat with 2 and 3 arguments → varchar.
#[test]
fn string_concat() {
    let sql2 = compile(r#"strs.map({a:=concat(s, t)})"#, STRS);
    assert_eq!(
        sql2,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, concat(v_1, v_2) as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
    let sql3 = compile(r#"strs.map({a:=concat(s, t, '!')})"#, STRS);
    assert_eq!(
        sql3,
        r#"select v_1 as "s", v_2 as "t", v_3 as "n", v_4 as "a" from (select v_1, v_2, v_3, concat(v_1, v_2, '!') as v_4 from (select "s" as v_1, "t" as v_2, "n" as v_3 from "strs") s) s"#
    );
}
