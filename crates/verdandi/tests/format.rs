use std::path::Path;
use std::time::Duration;

use tempfile::tempdir_in;
use tokio::time::sleep;
use verdandi::runner::Runner;

const KEY: &[u8] = b"cluster key";

fn dir() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c4t4-dirs");
    std::fs::create_dir_all(&root).unwrap();
    tempdir_in(root).unwrap()
}

async fn launch(dir: &Path, format_max: u16) -> std::io::Result<Runner> {
    Runner::launch_with(
        KEY,
        "127.0.0.1:0",
        Vec::new(),
        true,
        dir.into(),
        None,
        None,
        "http://127.0.0.1:1",
        format_max,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_below_the_committed_format_refuses_to_start_and_says_why() {
    let dir = dir();
    std::fs::create_dir_all(dir.path().join("cluster")).unwrap();
    std::fs::write(dir.path().join("cluster/committed"), [2u8]).unwrap();
    let err = launch(dir.path(), 1).await.err().unwrap();
    assert_eq!(
        err.to_string(),
        "the cluster format is 2; this build reads up to 1"
    );
    let runner = launch(dir.path(), 2).await.unwrap();
    sleep(Duration::from_millis(1500)).await;
    let committed = std::fs::read(dir.path().join("cluster/committed")).unwrap();
    assert_eq!(committed, [2u8]);
    runner.stop().await;
}
