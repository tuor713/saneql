/// Oracle-based golden tests for the SaneQL → SQL pipeline.
///
/// Phase 1: verify that the parser accepts every example file without error.
/// Phase 2: compile each example and compare against pre-generated golden SQL
///   files in `tests/golden/{category}/{name}.sql`.
use std::path::{Path, PathBuf};

mod tpch;

extern crate pollster;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn sane_files() -> Vec<PathBuf> {
    let root = manifest_dir();
    let examples = root.join("examples");
    let mut files = Vec::new();
    for subdir in &["tpch-sqlite", "features"] {
        let dir = examples.join(subdir);
        if !dir.exists() {
            continue;
        }
        let mut entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "sane"))
            .collect();
        entries.sort();
        files.extend(entries);
    }
    files
}

fn golden_sql(sane_path: &Path) -> PathBuf {
    let root = manifest_dir();
    let subdir = sane_path.parent().unwrap().file_name().unwrap();
    let stem = sane_path.file_stem().unwrap();
    root.join("tests/golden")
        .join(subdir)
        .join(stem)
        .with_extension("sql")
}

// ---------------------------------------------------------------------------
// Phase 1: parser smoke tests — every example must parse without error.
// ---------------------------------------------------------------------------

#[test]
fn all_examples_parse() {
    let files = sane_files();
    assert!(!files.is_empty(), "no .sane example files found");

    eprintln!("testing {} .sane files", files.len());
    let mut failures = Vec::new();
    for path in &files {
        let src = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        if let Err(msg) = saneql::parse(&src) {
            failures.push(format!("{}: {msg}", path.display()));
        }
    }

    if !failures.is_empty() {
        panic!(
            "{}/{} examples failed to parse:\n{}",
            failures.len(),
            files.len(),
            failures.join("\n")
        );
    }
}

// ---------------------------------------------------------------------------
// Phase 2: SQL generation golden tests.
// ---------------------------------------------------------------------------

/// Compare SQL output from the Rust implementation against pre-generated
/// golden files produced by `bin/saneql`.
#[test]
fn sql_output_matches_oracle() {
    let files = sane_files();
    let mut failures = Vec::new();
    let mut skipped = Vec::new();

    for path in &files {
        let p = path.as_path().display();
        println!("Testing {p}");
        let src = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));

        let actual_sql = match pollster::block_on(saneql::compile_with_schema(&src, tpch::tpch_schema())) {
            Ok(s) => s,
            Err(e) => {
                failures.push(format!("{}: compile error: {e}", path.display()));
                continue;
            }
        };

        let golden_path = golden_sql(path);
        if !golden_path.exists() {
            skipped.push(path.display().to_string());
            continue;
        }
        let expected_sql = std::fs::read_to_string(&golden_path)
            .unwrap_or_else(|e| panic!("cannot read golden {}: {e}", golden_path.display()))
            .trim()
            .to_string();

        if actual_sql.trim() != expected_sql {
            failures.push(format!(
                "{}:\n  expected: {}\n  actual:   {}",
                path.display(),
                expected_sql,
                actual_sql.trim()
            ));
        }
        println!("");
    }

    if !skipped.is_empty() {
        eprintln!("skipped (no golden file): {}", skipped.join(", "));
    }

    if !failures.is_empty() {
        panic!(
            "{}/{} golden tests failed:\n{}",
            failures.len(),
            files.len(),
            failures.join("\n\n")
        );
    }
}
