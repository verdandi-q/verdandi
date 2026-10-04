use std::fs;
use std::path::{Path, PathBuf};

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn i06_the_product_reads_no_argument_and_only_the_three_verdandi_variables() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    for name in [
        "verdandi",
        "verdandi-core",
        "verdandi-http",
        "verdandi-store",
    ] {
        sources(&crates.join(name).join("src"), &mut files);
    }
    let mut readers = Vec::new();
    for file in &files {
        let text = fs::read_to_string(file).expect("source");
        assert!(
            !text.contains("env::args"),
            "{} reads arguments",
            file.display()
        );
        if text.contains("env::var") {
            readers.push(file.clone());
        }
    }
    let main = crates.join("verdandi/src/main.rs");
    assert_eq!(readers, std::slice::from_ref(&main));
    let text = fs::read_to_string(main).expect("main");
    let mut named: Vec<&str> = text
        .split('"')
        .filter(|s| s.starts_with("VERDANDI_"))
        .collect();
    named.sort_unstable();
    assert_eq!(
        named,
        [
            "VERDANDI_BACKUP_URL",
            "VERDANDI_CLUSTER_KEY",
            "VERDANDI_CLUSTER_URL"
        ]
    );
}
