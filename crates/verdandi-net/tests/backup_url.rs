use object_store::{ObjectStoreExt, parse_url};
use url::Url;

#[tokio::test]
async fn file_url_parses_and_writes() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp/backup_url");
    std::fs::create_dir_all(&dir).unwrap();
    let dir = dir.canonicalize().unwrap();
    let url = Url::from_directory_path(&dir)
        .unwrap()
        .join("probe")
        .unwrap();
    assert_eq!(url.scheme(), "file");

    let (store, path) = parse_url(&url).unwrap();
    store.put(&path, "hello".into()).await.unwrap();

    let on_disk = std::fs::read(dir.join("probe")).unwrap();
    assert_eq!(on_disk, b"hello");
    let back = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(&back[..], b"hello");
}
