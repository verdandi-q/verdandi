use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempfile::{TempDir, tempdir_in};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};
use verdandi::runner::{Discovery, Runner, own_id};
use verdandi_core::Timing;
use verdandi_http::Node;
use verdandi_store::engine::{Engine, Handle, NewMessage, OsRandom, WallClock};
use verdandi_store::fs::RealFs;

const KEY: &[u8] = b"cluster key";
const BOUND: Duration = Duration::from_secs(120);
const LOCAL: Ipv4Addr = Ipv4Addr::LOCALHOST;

type Target = Arc<Mutex<Option<SocketAddr>>>;

fn tempdir() -> TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/prec6-dirs");
    std::fs::create_dir_all(&root).unwrap();
    tempdir_in(root).unwrap()
}

async fn forwarder() -> (String, Target) {
    let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let target: Target = Arc::default();
    let to = target.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut from, _)) = listener.accept().await else {
                return;
            };
            let Some(addr) = *to.lock().unwrap() else {
                continue;
            };
            tokio::spawn(async move {
                if let Ok(mut onward) = TcpStream::connect(addr).await {
                    let _ = tokio::io::copy_bidirectional(&mut from, &mut onward).await;
                }
            });
        }
    });
    (url, target)
}

struct Peer {
    runner: Runner,
    http: SocketAddr,
    target: Target,
    url: String,
}

async fn open(dir: &Path) -> Handle {
    let store = dir.join("store");
    std::fs::create_dir_all(&store).unwrap();
    let me = own_id(dir).unwrap();
    let (engine, handle) =
        Engine::open_as(RealFs, &store, Arc::new(WallClock), Box::new(OsRandom), me)
            .await
            .unwrap();
    tokio::spawn(engine.run());
    handle
}

async fn seed(dir: &Path) {
    let store = dir.join("store");
    std::fs::create_dir_all(&store).unwrap();
    let (engine, handle) = Engine::open(RealFs, &store, Arc::new(WallClock), Box::new(OsRandom))
        .await
        .unwrap();
    let task = tokio::spawn(engine.run());
    handle.publish(vec![message()], None).await.unwrap();
    handle.close().await.unwrap();
    task.await.unwrap().unwrap();
}

async fn start(dir: &Path, handle: Option<Handle>, reuse: Option<(String, Target)>) -> Peer {
    let fresh = reuse.is_none();
    let (url, target) = match reuse {
        Some(pair) => pair,
        None => forwarder().await,
    };
    let runner = Runner::launch(
        KEY,
        "127.0.0.1:0",
        Vec::new(),
        false,
        dir.into(),
        Some(Discovery {
            url: url.clone(),
            ip: LOCAL.into(),
        }),
        handle.clone(),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
    let http = listener.local_addr().unwrap();
    let node = Node::default();
    if let Some(handle) = handle {
        node.set(handle).ok().unwrap();
    }
    tokio::spawn(verdandi_http::serve_with(
        listener,
        node,
        Some(Arc::new(runner.cluster(KEY))),
        std::future::pending(),
    ));
    if fresh {
        *target.lock().unwrap() = Some(http);
    }
    Peer {
        runner,
        http,
        target,
        url,
    }
}

async fn both_found(a: &Peer, b: &Peer) {
    timeout(BOUND, async {
        while a.runner.members() != BTreeSet::from([a.runner.id()])
            || b.runner.members() != BTreeSet::from([b.runner.id()])
        {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("each node founds its own cluster");
}

fn message() -> NewMessage {
    NewMessage {
        stream: "s".to_owned(),
        headers: Vec::new(),
        value: b"data".to_vec(),
        timing: Timing::default(),
    }
}

async fn readyz(http: SocketAddr) -> u16 {
    reqwest::get(format!("http://{http}/readyz"))
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn the_loser_without_data_wipes_and_joins_the_winner() {
    let dirs = [tempdir(), tempdir()];
    let a = start(dirs[0].path(), None, None).await;
    let b = start(dirs[1].path(), None, None).await;
    both_found(&a, &b).await;
    let (win, lose, lose_dir) = if a.runner.id() < b.runner.id() {
        (a, b, dirs[1].path())
    } else {
        (b, a, dirs[0].path())
    };
    *lose.target.lock().unwrap() = Some(win.http);
    timeout(BOUND, lose.runner.failed())
        .await
        .expect("the loser stops");
    assert!(!lose_dir.join("cluster").exists());
    assert!(lose_dir.join("node-id").exists());
    let pair = (lose.url.clone(), lose.target.clone());
    let lose_id = lose.runner.id();
    lose.runner.stop().await;
    let back = start(lose_dir, None, Some(pair)).await;
    assert_eq!(back.runner.id(), lose_id);
    let all = BTreeSet::from([win.runner.id(), lose_id]);
    timeout(BOUND, async {
        loop {
            let leaders = BTreeSet::from([win.runner.leader(), back.runner.leader()]);
            if win.runner.members() == all
                && back.runner.members() == all
                && leaders.len() == 1
                && leaders.iter().all(|l| l.is_some_and(|l| all.contains(&l)))
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "no merge: {:?} {:?} {:?} {:?}",
            win.runner.leader(),
            win.runner.members(),
            back.runner.leader(),
            back.runner.members()
        )
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn the_loser_with_data_keeps_its_state_and_is_not_ready() {
    let dirs = [tempdir(), tempdir()];
    let ids = [
        own_id(dirs[0].path()).unwrap(),
        own_id(dirs[1].path()).unwrap(),
    ];
    let loser = usize::from(ids[1] > ids[0]);
    seed(dirs[loser].path()).await;
    let handle = open(dirs[loser].path()).await;
    let mut peers = [None, None];
    for i in [0, 1] {
        let with = (i == loser).then(|| handle.clone());
        peers[i] = Some(start(dirs[i].path(), with, None).await);
    }
    let [a, b] = peers.map(Option::unwrap);
    both_found(&a, &b).await;
    let (win, lose) = if loser == 1 { (a, b) } else { (b, a) };
    timeout(BOUND, async {
        while readyz(lose.http).await != 200 {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the loser is ready before the split");
    *lose.target.lock().unwrap() = Some(win.http);
    timeout(BOUND, async {
        while readyz(lose.http).await != 503 {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the loser is not ready");
    assert!(
        timeout(Duration::from_secs(15), lose.runner.failed())
            .await
            .is_err()
    );
    assert!(dirs[loser].path().join("cluster/control").exists());
    sleep(Duration::from_secs(30)).await;
    let (win_id, lose_id) = (win.runner.id(), lose.runner.id());
    assert_eq!(win.runner.members(), BTreeSet::from([win_id]));
    assert_eq!(lose.runner.members(), BTreeSet::from([lose_id]));
    assert_eq!(win.runner.leader(), Some(win_id));
    assert_eq!(lose.runner.leader(), Some(lose_id));
}
