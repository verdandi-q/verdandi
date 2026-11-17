use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN: [&str; 14] = [
    "std::{",
    "std::fs",
    "std::net",
    "std::io",
    "std::process",
    "std::env",
    "std::thread",
    "println!",
    "eprintln!",
    "tokio",
    "ring::",
    "use ring",
    "SystemTime::now",
    "Instant::now",
];

fn core_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../verdandi-core")
}

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn names_word(line: &str, word: &str) -> bool {
    line.match_indices(word)
        .any(|(i, _)| !line[..i].ends_with(|c: char| c.is_alphanumeric() || c == '_'))
}

fn io_words(text: &str) -> Vec<(usize, &'static str)> {
    let mut found = Vec::new();
    for (n, line) in text.lines().enumerate() {
        for word in FORBIDDEN.iter().filter(|w| names_word(line, w)) {
            found.push((n + 1, *word));
        }
    }
    found
}

#[test]
fn i10_core_source_names_no_io() {
    let mut files = Vec::new();
    sources(&core_dir().join("src"), &mut files);
    assert!(files.len() > 1, "no core sources found in {:?}", core_dir());
    let mut found = Vec::new();
    for file in files {
        let text = fs::read_to_string(&file).unwrap();
        for (n, word) in io_words(&text) {
            found.push(format!("{}:{n}: {word}", file.display()));
        }
    }
    assert!(found.is_empty(), "verdandi-core does I/O: {found:#?}");
}

#[test]
fn i10_a_grouped_std_import_is_refused() {
    for text in [
        "use std::{fs, net};",
        "use std::{\n    io::Write,\n};",
        "use ::std::{fs};",
    ] {
        assert!(!io_words(text).is_empty(), "{text}");
    }
    assert!(io_words("use std::collections::{BTreeMap, BTreeSet};").is_empty());
}

#[test]
fn i10_print_env_and_thread_are_refused() {
    for text in [
        "println!(\"x\");",
        "eprintln!(\"x\");",
        "let v = std::env::var(\"X\");",
        "use std::env;",
        "std::thread::spawn(f);",
        "use std::thread;",
    ] {
        assert!(!io_words(text).is_empty(), "{text}");
    }
}

#[test]
fn i10_core_has_no_dependencies() {
    let manifest = fs::read_to_string(core_dir().join("Cargo.toml")).unwrap();
    let mut in_deps = false;
    let mut deps = Vec::new();
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            in_deps = line.contains("dependencies") && !line.contains("dev-dependencies");
        } else if in_deps && !line.is_empty() {
            deps.push(line.to_owned());
        }
    }
    deps.retain(|d| !d.starts_with("serde = "));
    assert!(deps.is_empty(), "verdandi-core has dependencies: {deps:?}");
}
