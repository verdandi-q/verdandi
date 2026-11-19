use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::Level;
use verdandi::backup::{Backup, Source};
use verdandi::runner::{Discovery, Runner, own_id};
use verdandi_http::{Node, OPEN_MODE};
use verdandi_net::key_ok;
use verdandi_store::engine::{Engine, OsRandom, WallClock};
use verdandi_store::fs::RealFs;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

const KEY: &str = "VERDANDI_CLUSTER_KEY";
const URL: &str = "VERDANDI_CLUSTER_URL";
const BACKUP: &str = "VERDANDI_BACKUP_URL";

struct Cluster {
    key: String,
    url: Option<String>,
}

fn env(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(anyhow!("{name}: {e}")),
    }
}

fn bind_ip() -> anyhow::Result<IpAddr> {
    env("POD_IP")?.map_or(Ok(IpAddr::V4(Ipv4Addr::UNSPECIFIED)), |ip| {
        ip.parse().context("POD_IP is not an IP address")
    })
}

fn cluster() -> anyhow::Result<Option<Cluster>> {
    let (key, url) = (env(KEY)?, env(URL)?);
    let Some(key) = key else {
        if std::env::var_os(URL).is_some() {
            bail!("{URL} is set without {KEY}. Set {KEY}, or unset {URL} to start in open mode.");
        }
        return Ok(None);
    };
    key_ok(key.as_bytes()).map_err(|e| anyhow!("{KEY} is not valid: {e}"))?;
    if let Some(url) = &url {
        reqwest::Url::parse(url).with_context(|| format!("{URL} is not a URL"))?;
    }
    Ok(Some(Cluster { key, url }))
}

fn backup(key: &[u8]) -> anyhow::Result<Option<Backup>> {
    env(BACKUP)?
        .map(|url| Backup::open(&url, key).with_context(|| format!("{BACKUP} is not valid")))
        .transpose()
}

fn local_ip(url: &str) -> anyhow::Result<IpAddr> {
    if let Some(ip) = env("POD_IP")? {
        return ip.parse().context("POD_IP is not an IP address");
    }
    let url = reqwest::Url::parse(url)?;
    let host = url.host_str().ok_or_else(|| anyhow!("{URL} has no host"))?;
    let port = url.port_or_known_default().unwrap_or(80);
    let target = (host, port)
        .to_socket_addrs()
        .with_context(|| format!("resolve {host}"))?
        .next()
        .ok_or_else(|| anyhow!("{host} has no address"))?;
    let bind = if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind)?;
    socket.connect(target)?;
    Ok(socket.local_addr()?.ip())
}

fn main() -> anyhow::Result<()> {
    let cluster = cluster()?;
    let backup = backup(cluster.as_ref().map_or(&[][..], |c| c.key.as_bytes()))?;
    if cluster.is_none() {
        eprintln!("warning: {OPEN_MODE}");
    }
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
        .block_on(run(cluster, backup))
}

async fn run(cluster: Option<Cluster>, backup: Option<Backup>) -> anyhow::Result<()> {
    let ip = bind_ip()?;
    let address = SocketAddr::new(ip, 8080);
    let mut terminate = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    let dir = Path::new("data");
    std::fs::create_dir_all(dir).context("create ./data")?;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind {address}"))?;
    let me = match &cluster {
        Some(_) => own_id(dir).context("read ./data/node-id")?,
        None => 0,
    };
    let mut open = tokio::task::spawn_blocking(move || {
        Handle::current().block_on(Engine::open_as(
            RealFs,
            dir,
            Arc::new(WallClock),
            Box::new(OsRandom),
            me,
        ))
    });
    let mut early = None;
    let mut runner = None;
    let mut http_cluster = None;
    if let Some(c) = &cluster {
        let (engine, handle) = tokio::select! {
            opened = &mut open => opened.context("recover ./data")?.context("recover ./data")?,
            _ = terminate.recv() => std::process::exit(0),
        };
        let engine = tokio::spawn(engine.run());
        if backup.is_some() {
            handle
                .stored(Default::default())
                .await
                .context("hold compaction")?;
        }
        if let Some(backup) = &backup {
            verdandi::backup::restore(backup, &handle, dir, true).await?;
        }
        let listen = SocketAddr::new(ip, 7946).to_string();
        let found = c.url.is_none();
        let discovery = match &c.url {
            Some(url) => Some(Discovery {
                url: url.clone(),
                ip: local_ip(url)?,
            }),
            None => None,
        };
        let started = Runner::launch(
            c.key.as_bytes(),
            &listen,
            Vec::new(),
            found,
            dir.to_path_buf(),
            discovery,
            Some(handle.clone()),
        )
        .await
        .context("start the cluster node")?;
        http_cluster = Some(Arc::new(started.cluster(c.key.as_bytes())));
        runner = Some(Arc::new(started));
        early = Some((engine, handle));
    }
    let (stop, mut stopped) = watch::channel(false);
    let leaving = runner.clone();
    tokio::spawn(async move {
        terminate.recv().await;
        if let Some(runner) = leaving {
            runner.shutdown().await;
        }
        stop.send_replace(true);
    });
    let node = Node::default();
    let mut shutdown = stopped.clone();
    let server = tokio::spawn(verdandi_http::serve_with(
        listener,
        Arc::clone(&node),
        http_cluster,
        async move {
            shutdown.wait_for(|stop| *stop).await.ok();
        },
    ));
    tracing::info!("listening on {address}");
    let (engine, handle) = match early {
        Some(opened) => opened,
        None => tokio::select! {
            opened = open => {
                let (engine, handle) = opened.context("recover ./data")?.context("recover ./data")?;
                (tokio::spawn(engine.run()), handle)
            }
            Ok(_) = stopped.wait_for(|stop| *stop) => {
                tracing::info!("stopped before ready");
                std::process::exit(0);
            }
        },
    };
    if runner.is_none() && backup.is_some() {
        handle
            .stored(Default::default())
            .await
            .context("hold compaction")?;
    }
    if runner.is_none()
        && let Some(backup) = &backup
    {
        verdandi::backup::restore(backup, &handle, dir, false).await?;
    }
    if let Some(backup) = backup {
        let dir = dir.to_path_buf();
        match &runner {
            Some(runner) => runner.backup(backup, handle.clone(), dir),
            None => {
                tokio::spawn(verdandi::backup::run(
                    backup,
                    Source::alone(handle.clone(), dir),
                ));
            }
        }
    }
    let closer = handle.clone();
    node.set(handle)
        .map_err(|_| anyhow!("the node was made ready twice"))?;
    drop(node);
    tracing::info!("ready");
    tokio::spawn(async move {
        server.await.ok();
        closer.close().await.ok();
    });
    let watching = runner.clone();
    let ran = tokio::select! {
        done = engine => done
            .context("the engine task failed")?
            .context("the engine stopped"),
        () = async {
            match &watching {
                Some(runner) => runner.failed().await,
                None => std::future::pending().await,
            }
        } => Err(anyhow!("the cluster runner stopped")),
    };
    drop(runner);
    ran
}
