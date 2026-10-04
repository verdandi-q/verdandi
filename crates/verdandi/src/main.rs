use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::Level;
use verdandi_http::{Node, OPEN_MODE};
use verdandi_store::engine::{Engine, OsRandom, WallClock};
use verdandi_store::fs::RealFs;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

const CLUSTER: [&str; 3] = [
    "VERDANDI_CLUSTER_KEY",
    "VERDANDI_CLUSTER_URL",
    "VERDANDI_BACKUP_URL",
];
const ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8080);

fn main() -> anyhow::Result<()> {
    if let Some(name) = CLUSTER.iter().find(|n| std::env::var_os(n).is_some()) {
        bail!(
            "{name} is set, but this build runs one node only and has no cluster or backup. Unset {name} to start in open mode."
        );
    }
    eprintln!("warning: {OPEN_MODE}");
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .without_time()
        .with_writer(std::io::stderr)
        .init();
    let threads = std::thread::available_parallelism()?.get();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    let mut terminate = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    let (stop, mut stopped) = watch::channel(false);
    tokio::spawn(async move {
        terminate.recv().await;
        stop.send_replace(true);
    });
    let listener = TcpListener::bind(ADDRESS)
        .await
        .with_context(|| format!("bind {ADDRESS}"))?;
    let node = Node::default();
    let mut shutdown = stopped.clone();
    let server = tokio::spawn(verdandi_http::serve(
        listener,
        Arc::clone(&node),
        async move {
            shutdown.wait_for(|stop| *stop).await.ok();
        },
    ));
    tracing::info!("listening on {ADDRESS}");
    let dir = Path::new("data");
    std::fs::create_dir_all(dir).context("create ./data")?;
    let open = tokio::task::spawn_blocking(|| {
        Handle::current().block_on(Engine::open(
            RealFs,
            dir,
            Arc::new(WallClock),
            Box::new(OsRandom),
        ))
    });
    let (engine, handle) = tokio::select! {
        opened = open => opened.context("recover ./data")?.context("recover ./data")?,
        Ok(_) = stopped.wait_for(|stop| *stop) => {
            tracing::info!("stopped before ready");
            std::process::exit(0);
        }
    };
    let closer = handle.clone();
    node.set(handle)
        .map_err(|_| anyhow!("the node was made ready twice"))?;
    drop(node);
    tracing::info!("ready");
    tokio::spawn(async move {
        server.await.ok();
        closer.close().await.ok();
    });
    engine.run().await.context("the engine stopped")
}
